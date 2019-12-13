let reporter ?(prefix = "") () =
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let with_stamp h _tags k fmt =
      match level with
      | Logs.App -> Fmt.kpf k Fmt.stdout (fmt ^^ "@.%!")
      | _ ->
          let ppf = Fmt.stderr in
          let dt = Unix.gettimeofday () in
          Fmt.kpf k ppf
            ("%s%+04.0fus %a %a @[" ^^ fmt ^^ "@]@.")
            prefix dt
            Fmt.(styled `Magenta string)
            (Logs.Src.name src) Logs_fmt.pp_header (level, h)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k fmt
  in
  { Logs.report }

let src = Logs.Src.create "db_bench"

module Log = (val Logs.src_log src : Logs.LOG)

module Stats = Index.Stats

let src =
  let open Metrics in
  let open Stats in
  let tags = Tags.[] in
  let data t =
    Data.v
      [
        int "bytes_read" t.bytes_read;
        int "bytes_written" t.bytes_written;
        int "merge" t.nb_merge;
        int "replace" t.nb_replace;
      ]
  in
  Src.v "bench" ~tags ~data

let key_size = 32

let hash_size = 30

let value_size = 13

let entry_size = key_size + value_size

let nb_entries = 1_000_000

let log_size = 500_000

let ( // ) = Filename.concat

let random_char () = char_of_int (33 + Random.int 94)

let random_string string_size =
  String.init string_size (fun _i -> random_char ())

module Context = struct
  module Key = struct
    type t = string

    let v () = random_string key_size

    let hash = Hashtbl.hash

    let hash_size = hash_size

    let encode s = s

    let decode s off = String.sub s off key_size

    let encoded_size = key_size

    let equal = String.equal

    let pp s = Fmt.fmt "%s" s
  end

  module Value = struct
    type t = string

    let v () = random_string value_size

    let encode s = s

    let decode s off = String.sub s off value_size

    let encoded_size = value_size

    let pp s = Fmt.fmt "%s" s
  end
end

let with_stats f =
  Stats.reset_stats ();
  let t0 = Sys.time () in
  let x = f () in
  let t1 = Sys.time () -. t0 in
  let stats = Stats.get () in
  (x, t1, stats)

module Benchmark = struct
  type result = {
    time : float;
    ops_per_sec : float;
    mbs_per_sec : float;
    read_amplification_calls : float;
    read_amplification_size : float;
    write_amplification_calls : float;
    write_amplification_size : float;
  }
  [@@deriving yojson]

  let run ~with_metrics f =
    let _, time, stats = with_stats (fun () -> f ~with_metrics ()) in
    let nb_entriesf = Float.of_int nb_entries in
    let entry_sizef = Float.of_int entry_size in
    let read_amplification_size =
      Float.of_int stats.bytes_read /. (entry_sizef *. nb_entriesf)
    in
    let read_amplification_calls = Float.of_int stats.nb_reads /. nb_entriesf in
    let write_amplification_size =
      Float.of_int stats.bytes_written /. (entry_sizef *. nb_entriesf)
    in
    let write_amplification_calls =
      Float.of_int stats.nb_writes /. nb_entriesf
    in
    let ops_per_sec = nb_entriesf /. time in
    let mbs_per_sec = entry_sizef *. nb_entriesf /. 1_048_576. /. time in
    {
      time;
      ops_per_sec;
      mbs_per_sec;
      read_amplification_calls;
      read_amplification_size;
      write_amplification_calls;
      write_amplification_size;
    }

  let pp_result fmt result =
    Format.fprintf fmt
      "Total time: %f@\n\
       Operations per second: %f@\n\
       Mbytes per second: %f@\n\
       Read amplification in syscalls: %f@\n\
       Read amplification in bytes: %f@\n\
       Write amplification in syscalls: %f@\n\
       Write amplification in bytes: %f" result.time result.ops_per_sec
      result.mbs_per_sec result.read_amplification_calls
      result.read_amplification_size result.write_amplification_calls
      result.write_amplification_size

  let _pp_result_json fmt result =
    Yojson.Safe.pretty_print fmt (result_to_yojson result)
end

let make_bindings_pool () =
  Array.init nb_entries (fun _ ->
      let k = Context.Key.v () in
      let v = Context.Value.v () in
      (k, v))

let bindings_pool = ref [||]

let absent_bindings = ref [||]

module Index = struct
  module Index = Index_unix.Private.Make (Context.Key) (Context.Value)

  type index_v = fresh:bool -> readonly:bool -> string -> Index.t

  type benchmark = with_metrics:bool -> index_v -> unit -> unit

  let add_metrics =
    let no_tags x = x in
    fun () -> Metrics.add src no_tags (fun m -> m (Stats.get ()))

  let write ~with_metrics ?(with_flush = false) bindings rw =
    Array.iter
      (fun (k, v) ->
        Index.replace rw k v;
        if with_flush then Index.flush rw;
        if with_metrics then add_metrics ())
      bindings

  let read ~with_metrics bindings r =
    Array.iter
      (fun (k, (_ : Context.Value.t)) ->
        ignore (Index.find r k : Context.Value.t);
        if with_metrics then add_metrics ())
      bindings

  let read_absent ~with_metrics bindings r =
    Array.iter
      (fun (k, _) ->
        try
          ignore (Index.find r k : Context.Value.t);
          failwith "Absent value found"
        with Not_found -> if with_metrics then add_metrics ())
      bindings

  let write_random ~with_metrics v () =
    v ~fresh:true ~readonly:false "" |> write ~with_metrics !bindings_pool

  let write_seq ~with_metrics v =
    let bindings = Array.copy !bindings_pool in
    Array.sort (fun a b -> String.compare (fst a) (fst b)) bindings;
    fun () -> v ~fresh:true ~readonly:false "" |> write ~with_metrics bindings

  let write_seq_hash ~with_metrics v =
    let hash e = Context.Key.hash (fst e) in
    let bindings = Array.copy !bindings_pool in
    Array.sort (fun a b -> Int.compare (hash a) (hash b)) bindings;
    fun () -> v ~fresh:true ~readonly:false "" |> write ~with_metrics bindings

  let write_rev_seq_hash ~with_metrics v =
    let hash e = Context.Key.hash (fst e) in
    let bindings = Array.copy !bindings_pool in
    Array.sort (fun a b -> -Int.compare (hash a) (hash b)) bindings;
    fun () -> v ~fresh:true ~readonly:false "" |> write ~with_metrics bindings

  let write_sync ~with_metrics v () =
    v ~fresh:true ~readonly:false ""
    |> write ~with_metrics ~with_flush:true !bindings_pool

  let iter ~with_metrics v () =
    v ~fresh:false ~readonly:true ""
    |> Index.iter (fun _ _ -> if with_metrics then add_metrics ())

  let find_random ~with_metrics v () =
    v ~fresh:false ~readonly:false "" |> read ~with_metrics !bindings_pool

  let find_random_ro ~with_metrics v () =
    v ~fresh:false ~readonly:true "" |> read ~with_metrics !bindings_pool

  let find_absent ~with_metrics v () =
    v ~fresh:false ~readonly:false ""
    |> read_absent ~with_metrics !absent_bindings

  let find_absent_ro ~with_metrics v () =
    v ~fresh:false ~readonly:true ""
    |> read_absent ~with_metrics !absent_bindings

  let run :
      with_metrics:bool ->
      root:string ->
      name:string ->
      benchmark ->
      Benchmark.result =
   fun ~with_metrics ~root ~name b ->
    let indices = ref [] in
    let index_v ~fresh ~readonly n =
      let i = Index.v ~fresh ~readonly ~log_size (root // name // n) in
      indices := i :: !indices;
      i
    in
    let result = Benchmark.run ~with_metrics (b index_v) in
    !indices |> List.iter (fun i -> Index.close i);
    result

  type suite_elt = {
    name : string;
    synopsis : string;
    benchmark : benchmark;
    dependency : string option;
  }

  let suite =
    [
      {
        name = "replace_random";
        synopsis = "Replace in random order";
        benchmark = write_random;
        dependency = None;
      };
      {
        name = "replace_random_sync";
        synopsis = "Replace in random order with sync";
        benchmark = write_sync;
        dependency = None;
      };
      {
        name = "replace_increasing_keys";
        synopsis = "Replace in increasing order of keys";
        benchmark = write_seq;
        dependency = None;
      };
      {
        name = "replace_increasing_hash";
        synopsis = "Replace in increasing order of hash";
        benchmark = write_seq_hash;
        dependency = None;
      };
      {
        name = "replace_decreasing_hash";
        synopsis = "Replace in decreasing order of hashes";
        benchmark = write_rev_seq_hash;
        dependency = None;
      };
      {
        name = "iter_rw";
        synopsis = "[RW] Iter";
        benchmark = iter;
        dependency = Some "replace_random";
      };
      {
        name = "find_random_ro";
        synopsis = "[RO] Find in random order";
        benchmark = find_random_ro;
        dependency = Some "replace_random";
      };
      {
        name = "find_random_rw";
        synopsis = "[RW] Find in random order";
        benchmark = find_random;
        dependency = Some "replace_random";
      };
      {
        name = "find_absent_ro";
        synopsis = "[RO] Find absent values";
        benchmark = find_absent_ro;
        dependency = Some "replace_random";
      };
      {
        name = "find_absent_rw";
        synopsis = "[RW] Find absent values";
        benchmark = find_absent;
        dependency = Some "replace_random";
      };
    ]
end

let list_benchs () =
  let pp_bench ppf b = Fmt.pf ppf "%s\t-- %s" b.Index.name b.synopsis in
  Index.suite |> Fmt.(pr "%a" (list ~sep:Fmt.(const string "\n") pp_bench))

let schedule p s =
  let todos = List.map fst in
  let init = ref (s |> List.map (fun b -> (p b.Index.name, b))) in
  let apply_dep s =
    let deps =
      s
      |> List.fold_left
           (fun acc (todo, b) ->
             if todo then
               match b.Index.dependency with Some s -> s :: acc | None -> acc
             else acc)
           []
    in
    s |> List.map (fun (todo, b) -> (todo || List.mem b.Index.name deps, b))
  in
  let next = ref (apply_dep !init) in
  while todos !init <> todos !next do
    init := !next;
    next := apply_dep !init
  done;
  let r = List.filter fst !init |> List.map snd in
  r

let init with_metrics seed =
  Logs.set_level (Some Logs.App);
  Logs.set_reporter (reporter ());
  Random.init seed;
  if with_metrics then (
    Metrics.enable_all ();
    Metrics_gnuplot.set_reporter ();
    Metrics_unix.monitor_gc 0.1 );
  Log.app (fun l -> l "Size of keys: %d bytes." key_size);
  Log.app (fun l -> l "Size of values: %d bytes." value_size);
  Log.app (fun l -> l "Number of bindings: %d." nb_entries);
  Log.app (fun l -> l "Log size: %d." log_size);
  Log.app (fun l -> l "Initializing data with seed %d.\n" seed);
  bindings_pool := make_bindings_pool ();
  absent_bindings := make_bindings_pool ()

let run filter data_dir seed with_metrics =
  init with_metrics seed;
  let name_filter =
    match filter with None -> fun _ -> true | Some re -> Re.execp re
  in
  Index.suite
  |> schedule name_filter
  |> List.iter (fun Index.{ synopsis; name; benchmark; dependency } ->
         let name = match dependency with None -> name | Some name -> name in
         let result = Index.run ~with_metrics ~root:data_dir ~name benchmark in
         Logs.app (fun l ->
             l "%s@\n    @[%a@]@\n" synopsis Benchmark.pp_result result))

open Cmdliner

let regex =
  let parse s =
    try Ok Re.(compile @@ Pcre.re s) with
    | Re.Perl.Parse_error -> Error (`Msg "Perl-compatible regexp parse error")
    | Re.Perl.Not_supported -> Error (`Msg "unsupported regexp feature")
  in
  let print = Re.pp_re in
  Arg.conv (parse, print)

let name_filter =
  let doc = "A regular expression matching the names of benchmarks to run" in
  Arg.(
    value
    & opt (some regex) None
    & info [ "f"; "filter" ] ~doc ~docv:"NAME_REGEX")

let data_dir =
  let doc = "Set directory for the data files" in
  Arg.(value & opt dir "_bench" & info [ "d"; "directory" ] ~doc)

let seed =
  let doc = "The seed used to generate random data." in
  Arg.(value & opt int 0 & info [ "s"; "seed" ] ~doc)

let metrics_flag =
  let doc = "Use Metrics; note that it has an impact on performance" in
  Arg.(value & flag & info [ "m"; "with_metrics" ] ~doc)

let list_cmd =
  let doc = "List all available benchmarks." in
  (Term.(pure list_benchs $ const ()), Term.info "list" ~doc)

let cmd =
  let doc = "Run all the benchmarks." in
  ( Term.(const run $ name_filter $ data_dir $ seed $ metrics_flag),
    Term.info "run" ~doc ~exits:Term.default_exits )

let () =
  let choices = [ list_cmd ] in
  Term.(exit @@ eval_choice cmd choices)
