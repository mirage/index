include Checks_intf

module Make (K : Data.Key) (V : Data.Value) (IO : Io.S) = struct
  module Entry = Data.Entry.Make (K) (V)

  module IO = struct
    include Io.Extend (IO)

    (** This module never makes persistent changes *)
    let v =
      v ~fresh:false ~readonly:true ?flush_callback:None ~generation:0L
        ~fan_size:0L

    let page_size = Int64.(mul (of_int Entry.encoded_size) 1000L)

    let iter ?min ?max f =
      iter ?min ?max ~page_size (fun ~off ~buf ~buf_off ->
          let entry = Entry.decode buf buf_off in
          f off entry;
          Entry.encoded_size)

    let read_entry io off =
      let buf = Bytes.create Entry.encoded_size in
      let (_ : int) = IO.read io ~off ~len:Entry.encoded_size buf in
      Entry.decode (Bytes.unsafe_to_string buf) 0
  end

  type size = Bytes of int [@@deriving repr]

  let size_t =
    let pp =
      Fmt.using (fun (Bytes b) -> Int64.of_int b) Progress.Units.Bytes.pp
    in
    Repr.like
      ~json:
        ( (fun e t ->
            ignore @@ Jsonm.encode e (`Lexeme (`String (Fmt.to_to_string pp t)))),
          fun _ -> assert false )
      size_t

  let path =
    let open Cmdliner.Arg in
    required
    @@ pos 0 (some string) None
    @@ info ~doc:"Path to the Index store on disk" ~docv:"PATH" []

  module Stat = struct
    type io = { size : size; offset : int64; generation : int64 }
    [@@deriving repr]

    type files = {
      data : io option;
      log : io option;
      log_async : io option;
      merge : io option;
      lock : string option;
    }
    [@@deriving repr]

    type t = { entry_size : size; files : files } [@@deriving repr]

    let with_io : type a. string -> (IO.t -> a) -> a option =
     fun path f ->
      match IO.exists path with
      | false -> None
      | true ->
          let io = IO.v path in
          let a = f io in
          IO.close io;
          Some a

    let io path =
      with_io path @@ fun io ->
      let IO.Header.{ offset; generation } = IO.Header.get io in
      let size = Bytes (IO.size io) in
      { size; offset; generation }

    let run ~root =
      Logs.app (fun f -> f "Getting statistics for store: `%s'@," root);
      let data = io (Layout.data ~root) in
      let log = io (Layout.log ~root) in
      let log_async = io (Layout.log_async ~root) in
      let merge = io (Layout.merge ~root) in
      let lock =
        IO.Lock.pp_dump (Layout.lock ~root)
        |> Option.map (fun f ->
               f Format.str_formatter;
               Format.flush_str_formatter ())
      in
      {
        entry_size = Bytes (K.encoded_size + V.encoded_size);
        files = { data; log; log_async; merge; lock };
      }
      |> Repr.pp_json ~minify:false t Fmt.stdout

    let term = Cmdliner.Term.(const (fun root () -> run ~root) $ path)
  end

  module Integrity_check = struct
    let encoded_sizeL = Int64.of_int Entry.encoded_size

    let print_window_around central_offset io context =
      let window_size = (2 * context) + 1 in
      List.init window_size (fun i ->
          let index = i - context in
          Int64.(add central_offset (mul (of_int index) encoded_sizeL)))
      |> List.filter (fun off -> Int64.compare off 0L >= 0)
      |> List.map (fun off ->
             let entry = IO.read_entry io off in
             let highlight =
               if off = central_offset then Fmt.(styled (`Fg `Red)) else Fun.id
             in
             highlight (fun ppf () -> (Repr.pp Entry.t) ppf entry))
      |> Fmt.(concat ~sep:cut)

    let run ~root =
      let context = 2 in
      let io = IO.v (Layout.data ~root) in
      let io_offset = IO.offset io in
      if Int64.compare io_offset encoded_sizeL < 0 then (
        if not (Int64.equal io_offset 0L) then
          Fmt.failwith
            "Non-integer number of entries in file: { offset = %Ld; entry_size \
             = %d }"
            io_offset Entry.encoded_size)
      else
        let first_entry = IO.read_entry io 0L in
        let previous = ref first_entry in
        Format.eprintf "\n%!";
        Progress_unix.(
          counter ~total:io_offset ~mode:`UTF8
            ~message:"Scanning store for faults" ~pp:Progress.Units.bytes
            ~sampling_interval:100_000 ()
          |> with_reporters)
        @@ fun report ->
        io
        |> IO.iter ~min:encoded_sizeL (fun off e ->
               report encoded_sizeL;
               if !previous.key_hash > e.key_hash then
                 Log.err (fun f ->
                     f "Found non-monotonic region:@,%a@,"
                       (print_window_around off io context)
                       ());
               previous := e)

    let term = Cmdliner.Term.(const (fun root () -> run ~root) $ path)
  end

  module Cli = struct
    open Cmdliner

    let setup_log =
      let init style_renderer level =
        let format_reporter =
          let report _src level ~over k msgf =
            let k _ =
              over ();
              k ()
            in
            msgf @@ fun ?header:_ ?tags:_ fmt ->
            match level with
            | Logs.App ->
                Fmt.kpf k Fmt.stderr
                  ("@[<v 0>%a" ^^ fmt ^^ "@]@.")
                  Fmt.(styled `Bold (styled (`Fg `Cyan) string))
                  ">> "
            | _ -> Fmt.kpf k Fmt.stdout ("@[<v 0>" ^^ fmt ^^ "@]@.")
          in
          { Logs.report }
        in
        Fmt_tty.setup_std_outputs ?style_renderer ();
        Logs.set_level level;
        Logs.set_reporter format_reporter
      in
      Term.(const init $ Fmt_cli.style_renderer () $ Logs_cli.level ())

    let main () : empty =
      let default =
        let default_info =
          let doc = "Check and repair Index data-stores." in
          Term.info ~doc "index-fsck"
        in
        Term.(ret (const (`Help (`Auto, None))), default_info)
      in
      Term.(
        eval_choice default
          [
            ( Stat.term $ setup_log,
              Term.info ~doc:"Print high-level statistics about the store."
                "stat" );
            ( Integrity_check.term $ setup_log,
              Term.info
                ~doc:"Search the store for integrity faults and corruption."
                "integrity-check" );
          ]
        |> (exit : unit result -> _));
      assert false
  end

  let cli = Cli.main
end
