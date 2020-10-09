include Checks_intf
module T = Irmin_type.Type

(** Offline integrity checking and recovery for an Index store *)

module Make (K : Data.Key) (V : Data.Value) (Io : Io.S) = struct
  (** This module never makes persistent changes *)
  module Io = struct
    include Io

    let v =
      v ~fresh:false ~readonly:true ?flush_callback:None ~generation:0L
        ~fan_size:0L
  end

  type size = Bytes of int [@@deriving irmin]

  let size_t =
    let pp =
      (* Round down to the nearest 0.1 *)
      let trunc f = Float.trunc (f *. 10.) /. 10. in
      fun ppf (Bytes x) ->
        Int.to_float x |> function
        | n when n < 1024. -> Format.fprintf ppf "%.0f B" (trunc n)
        | n when n < 1024. ** 2. ->
            Format.fprintf ppf "%.1f KiB" (trunc (n /. 1024.))
        | n when n < 1024. ** 3. ->
            Format.fprintf ppf "%.1f MiB" (trunc (n /. (1024. ** 2.)))
        | n -> Format.fprintf ppf "%.1f GiB" (trunc (n /. (1024. ** 3.)))
    in
    T.like
      ~json:
        ( (fun e t ->
            ignore @@ Jsonm.encode e (`Lexeme (`String (Fmt.to_to_string pp t)))),
          fun _ -> assert false )
      size_t

  (** Read basic metrics from an existing store. *)
  module Quick_stats = struct
    type io = { size : size; offset : int64; generation : int64 }
    [@@deriving irmin]

    type files = {
      data : io option;
      log : io option;
      log_async : io option;
      merge : io option;
      lock : string option;
    }
    [@@deriving irmin]

    type t = { entry_size : size; files : files } [@@deriving irmin]

    let with_io : type a. string -> (Io.t -> a) -> a option =
     fun path f ->
      match Io.exists path with
      | false -> None
      | true ->
          let io = Io.v path in
          let a = f io in
          Io.close io;
          Some a

    let io path =
      with_io path @@ fun io ->
      let Io.Header.{ offset; generation } = Io.Header.get io in
      let size = Bytes (Io.size io) in
      { size; offset; generation }

    let v ~root =
      Logs.app (fun f -> f "Getting statistics for store: `%s'@," root);
      let data = io (Layout.data ~root) in
      let log = io (Layout.log ~root) in
      let log_async = io (Layout.log_async ~root) in
      let merge = io (Layout.merge ~root) in
      let lock =
        Io.Lock.pp_dump (Layout.lock ~root)
        |> Option.map (fun f ->
               f Format.str_formatter;
               Format.flush_str_formatter ())
      in
      {
        entry_size = Bytes (K.encoded_size + V.encoded_size);
        files = { data; log; log_async; merge; lock };
      }
      |> T.pp_json ~minify:false t Fmt.stdout
  end

  (** Run extensive checks on the store:

      - ensure that the [data] file is monotonic *)
  module Integrity_check = struct
    let v ~root:_ = ()
  end

  module Cli = struct
    open Cmdliner

    let path =
      let open Arg in
      required
      @@ pos 0 (some string) None
      @@ info ~doc:"Path to the Index store on disk" ~docv:"PATH" []

    let quick_stats = Term.(const (fun root () -> Quick_stats.v ~root) $ path)

    let integrity_check =
      Term.(const (fun root () -> Integrity_check.v ~root) $ path)

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
                  ("%a@[<v 0>" ^^ fmt ^^ "@]@.")
                  Fmt.(styled `Bold (styled (`Fg `Cyan) string))
                  ">> "
            | _ -> Fmt.kpf k Fmt.stdout fmt
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
            ( quick_stats $ setup_log,
              Term.info ~doc:"Print high-level statistics about the store."
                "stat" );
            ( integrity_check $ setup_log,
              Term.info
                ~doc:"Search the store for integrity faults and corruption."
                "check" );
          ]
        |> (exit : unit result -> _));
      assert false
  end

  let cli = Cli.main
end
