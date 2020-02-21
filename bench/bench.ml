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

let value_size = 13

let entry_size = key_size + value_size

let ( // ) = Filename.concat

let random_char () = char_of_int (33 + Random.int 94)

let random_string string_size =
  String.init string_size (fun _i -> random_char ())

module Context = struct
  module Key = struct
    type t = string

    let v () = random_string key_size

    let hash = Hashtbl.hash

    let hash_size = 30

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
  let _ = f () in
  let t1 = Sys.time () -. t0 in
  let stats = Stats.get () in
  (t1, stats)

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

  let run ~nb_entries f =
    let time, stats = with_stats (fun () -> f ()) in
    let nb_entriesf = float_of_int nb_entries in
    let entry_sizef = float_of_int entry_size in
    let read_amplification_size =
      float_of_int stats.bytes_read /. (entry_sizef *. nb_entriesf)
    in
    let read_amplification_calls = float_of_int stats.nb_reads /. nb_entriesf in
    let write_amplification_size =
      float_of_int stats.bytes_written /. (entry_sizef *. nb_entriesf)
    in
    let write_amplification_calls =
      float_of_int stats.nb_writes /. nb_entriesf
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
end

let make_bindings_pool nb_entries =
  Array.init nb_entries (fun _ ->
      let k = Context.Key.v () in
      let v = Context.Value.v () in
      (k, v))

let bindings_pool = ref [||]

let absent_bindings_pool = ref [||]

let sorted_bindings_pool = ref [||]

module Index = struct
  module Index = Index_unix.Private.Make (Context.Key) (Context.Value)

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
      (fun (k, _) ->
        ignore (Index.find r k : Context.Value.t);
        if with_metrics then add_metrics ())
      bindings

  let read_absent ~with_metrics bindings r =
    Array.iter
      (fun (k, _) ->
        try ignore (Index.find r k : Context.Value.t)
        with Not_found -> if with_metrics then add_metrics ())
      bindings

  let write_random ~with_metrics t () = write ~with_metrics !bindings_pool t

  let write_seq ~with_metrics t =
    Array.sort (fun a b -> String.compare (fst a) (fst b)) !sorted_bindings_pool;
    fun () -> write ~with_metrics !sorted_bindings_pool t

  let write_seq_hash ~with_metrics t =
    let hash e = Context.Key.hash (fst e) in
    Array.sort (fun a b -> compare (hash a) (hash b)) !sorted_bindings_pool;
    fun () -> write ~with_metrics !sorted_bindings_pool t

  let write_rev_seq_hash ~with_metrics t =
    let hash e = Context.Key.hash (fst e) in
    Array.sort (fun a b -> -compare (hash a) (hash b)) !sorted_bindings_pool;
    fun () -> write ~with_metrics !sorted_bindings_pool t

  let write_sync ~with_metrics t () =
    write ~with_metrics ~with_flush:true !bindings_pool t

  let iter ~with_metrics t () =
    Index.iter (fun _ _ -> if with_metrics then add_metrics ()) t

  let find_random ~with_metrics t () = read ~with_metrics !bindings_pool t

  let find_absent ~with_metrics t () =
    read_absent ~with_metrics !absent_bindings_pool t

  let run ~with_metrics ~nb_entries ~log_size ~root ~name ~fresh ~readonly b =
    let index = Index.v ~fresh ~readonly ~log_size (root // name) in
    let result = Benchmark.run ~nb_entries (b ~with_metrics index) in
    Index.close index;
    result

  type suite_elt = {
    name : string;
    synopsis : string;
    readonly : bool;
    fresh : bool;
    benchmark : with_metrics:bool -> Index.t -> unit -> unit;
    dependency : string option;
  }

  let suite =
    [
      {
        name = "replace_random";
        synopsis = "Replace in random order";
        readonly = false;
        fresh = true;
        benchmark = write_random;
        dependency = None;
      };
      {
        name = "replace_random_sync";
        synopsis = "Replace in random order with sync";
        readonly = false;
        fresh = true;
        benchmark = write_sync;
        dependency = None;
      };
      {
        name = "replace_increasing_keys";
        synopsis = "Replace in increasing order of keys";
        readonly = false;
        fresh = true;
        benchmark = write_seq;
        dependency = None;
      };
      {
        name = "replace_increasing_hash";
        synopsis = "Replace in increasing order of hashes";
        readonly = false;
        fresh = true;
        benchmark = write_seq_hash;
        dependency = None;
      };
      {
        name = "replace_decreasing_hash";
        synopsis = "Replace in decreasing order of hashes";
        readonly = false;
        fresh = true;
        benchmark = write_rev_seq_hash;
        dependency = None;
      };
      {
        name = "iter_rw";
        synopsis = "[RW] Iter";
        readonly = false;
        fresh = false;
        benchmark = iter;
        dependency = Some "replace_random";
      };
      {
        name = "find_random_ro";
        synopsis = "[RO] Find in random order";
        readonly = true;
        fresh = false;
        benchmark = find_random;
        dependency = Some "replace_random";
      };
      {
        name = "find_random_rw";
        synopsis = "[RW] Find in random order";
        readonly = false;
        fresh = false;
        benchmark = find_random;
        dependency = Some "replace_random";
      };
      {
        name = "find_absent_ro";
        synopsis = "[RO] Find absent values";
        readonly = true;
        fresh = false;
        benchmark = find_absent;
        dependency = Some "replace_random";
      };
      {
        name = "find_absent_rw";
        synopsis = "[RW] Find absent values";
        readonly = false;
        fresh = false;
        benchmark = find_absent;
        dependency = Some "replace_random";
      };
    ]
end

let list_benches () =
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

type config = {
  key_size : int;
  value_size : int;
  nb_entries : int;
  log_size : int;
  seed : int;
  with_metrics : bool;
}
[@@deriving yojson]

let pp_config fmt config =
  Format.fprintf fmt
    "Key size: %d@\n\
     Value size: %d@\n\
     Number of bindings: %d@\n\
     Log size: %d@\n\
     Seed: %d@\n\
     Metrics: %b" config.key_size config.value_size config.nb_entries
    config.log_size config.seed config.with_metrics

let cleanup root =
  let files = [ "data"; "log"; "lock"; "log_async"; "merge" ] in
  List.iter
    (fun (b : Index.suite_elt) ->
      let dir = root // b.name // "index" in
      List.iter
        (fun file ->
          let file = dir // file in
          if Sys.file_exists file then Unix.unlink file)
        files)
    Index.suite

let init config =
  Printexc.record_backtrace true;
  Random.init config.seed;
  if config.with_metrics then (
    Metrics.enable_all ();
    Metrics_gnuplot.set_reporter ();
    Metrics_unix.monitor_gc 0.1 );
  bindings_pool := make_bindings_pool config.nb_entries;
  absent_bindings_pool := make_bindings_pool config.nb_entries;
  sorted_bindings_pool := Array.copy !bindings_pool

let print fmt (config, results) =
  let pp_bench fmt (b, result) =
    Format.fprintf fmt "%s@\n    @[%a@]" b.Index.synopsis Benchmark.pp_result
      result
  in
  Format.fprintf fmt "Configuration:@\n    @[%a@]@\n@\nResults:@\n    @[%a@]@\n"
    pp_config config
    Fmt.(list ~sep:(any "@\n@\n") pp_bench)
    results

let print_json fmt (config, results) =
  let open Yojson.Safe in
  let obj =
    `Assoc
      [
        ("config", config_to_yojson config);
        ( "results",
          `List
            (List.map
               (fun (b, result) ->
                 `Assoc
                   [
                     ("name", `String b.Index.name);
                     ("metrics", Benchmark.result_to_yojson result);
                   ])
               results) );
      ]
  in
  pretty_print fmt obj

let run filter root output seed with_metrics log_size nb_entries json =
  let config =
    { key_size; value_size; nb_entries; log_size; seed; with_metrics }
  in
  cleanup root;
  init config;
  let name_filter =
    match filter with None -> fun _ -> true | Some re -> Re.execp re
  in
  Index.suite
  |> schedule name_filter
  |> List.map (fun (b : Index.suite_elt) ->
         let name =
           match b.dependency with None -> b.name | Some name -> name
         in
         let result =
           Index.run ~with_metrics ~nb_entries ~log_size ~root ~name
             ~fresh:b.fresh ~readonly:b.readonly b.benchmark
         in
         (b, result))
  |> fun results ->
  let fmt =
    (match output with None -> stdout | Some filename -> open_out filename)
    |> Format.formatter_of_out_channel
  in
  Fmt.pf fmt "%a@." (if json then print_json else print) (config, results)

open Cmdliner

let env_var s = Arg.env_var ("INDEX_BENCH_" ^ s)

let new_file =
  let parse s =
    match Sys.file_exists s && Sys.is_directory s with
    | false -> `Ok s
    | true -> `Error (Printf.sprintf "Error: `%s' is a directory" s)
  in
  (parse, Format.pp_print_string)

let regex =
  let parse s =
    try Ok Re.(compile @@ Pcre.re ~flags:[ `ANCHORED ] s) with
    | Re.Perl.Parse_error -> Error (`Msg "Perl-compatible regexp parse error")
    | Re.Perl.Not_supported -> Error (`Msg "unsupported regexp feature")
  in
  let print = Re.pp_re in
  Arg.conv (parse, print)

let name_filter =
  let doc =
    "A regular expression matching the names of benchmarks to run. For more \
     information about the regexp syntax, please visit \
     https://perldoc.perl.org/perlre.html#Regular-Expressions."
  in
  let env = env_var "NAME_FILTER" in
  Arg.(
    value
    & opt (some regex) None
    & info [ "f"; "filter" ] ~env ~doc ~docv:"NAME_REGEX")

let data_dir =
  let doc = "Set directory for the data files" in
  let env = env_var "DATA_DIR" in
  Arg.(value & opt dir "_bench" & info [ "d"; "data-dir" ] ~env ~doc)

let output =
  let doc = "Specify an output file where the results should be written" in
  let env = env_var "OUTPUT" in
  Arg.(value & opt (some new_file) None & info [ "o"; "output" ] ~env ~doc)

let seed =
  let doc = "The seed used to generate random data." in
  let env = env_var "SEED" in
  Arg.(value & opt int 0 & info [ "s"; "seed" ] ~env ~doc)

let metrics_flag =
  let doc = "Use Metrics; note that it has an impact on performance" in
  let env = env_var "WITH_METRICS" in
  Arg.(value & flag & info [ "m"; "with-metrics" ] ~env ~doc)

let log_size =
  let doc = "The log size of the index." in
  let env = env_var "LOG_SIZE" in
  Arg.(value & opt int 500_000 & info [ "log-size" ] ~env ~doc)

let nb_entries =
  let doc = "The number of bindings." in
  let env = env_var "NB_ENTRIES" in
  Arg.(value & opt int 10_000_000 & info [ "nb-entries" ] ~env ~doc)

let list_cmd =
  let doc = "List all available benchmarks." in
  (Term.(pure list_benches $ const ()), Term.info "list" ~doc)

let json_flag =
  let doc = "Output the results as a json object." in
  let env = env_var "JSON" in
  Arg.(value & flag & info [ "j"; "json" ] ~env ~doc)

let cmd =
  let doc = "Run all the benchmarks." in
  ( Term.(
      const run
      $ name_filter
      $ data_dir
      $ output
      $ seed
      $ metrics_flag
      $ log_size
      $ nb_entries
      $ json_flag),
    Term.info "run" ~doc ~exits:Term.default_exits )

let () =
  let choices = [ list_cmd ] in
  Term.(exit @@ eval_choice cmd choices)
