module Stats = Index.Stats

let src =
  let open Metrics in
  let string_of_head ls =
    if List.length ls <> 0 then string_of_float (List.hd ls) else "0"
  in
  let head k v =
    field ~doc:"Time of the latest replace" k
      (Other (Fmt.of_to_string string_of_head))
      v
  in
  let open Stats in
  let tags = Tags.[] in
  let data t =
    Data.v
      [
        int "bytes_read" t.bytes_read;
        int "bytes_written" t.bytes_written;
        int "merge" t.nb_merge;
        int "replace" t.nb_replace;
        head "replace_durations" t.replace_durations;
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

let replace_sampling_interval = ref 0

module Context = struct
  module Key = struct
    type t = string [@@deriving repr]

    let v () = random_string key_size
    let hash = Hashtbl.hash
    let hash_size = 30
    let encode s = s
    let decode s off = String.sub s off key_size
    let encoded_size = key_size
    let equal = String.equal
  end

  module Value = struct
    type t = string [@@deriving repr]

    let v () = random_string value_size
    let encode s = s
    let decode s off = String.sub s off value_size
    let encoded_size = value_size
  end
end

let with_stats f =
  Stats.reset_stats ();
  let _, duration = Common.with_timer f in
  let stats = Stats.get () in
  (duration, stats)

module Mtime = struct
  include Mtime

  module Span = struct
    include Span

    let to_yojson s = `String (Fmt.to_to_string pp s)
    let div a b = of_uint64_ns (Int64.div (to_uint64_ns a) (Int64.of_int b))
  end
end

module Benchmark = struct
  type result = {
    time : Mtime.Span.t;
    ops_per_sec : float;
    mbs_per_sec : float;
    read_amplification_calls : float;
    read_amplification_size : float;
    write_amplification_calls : float;
    write_amplification_size : float;
    replace_durations : float list;
    merges_duration : float; [@key "merges_duration_us"]
    nb_merges : int;
  }
  [@@deriving to_yojson]

  let run ~nb_entries f =
    let time, stats = with_stats (fun () -> f ()) in
    let time_sec = Mtime.Span.to_float_ns time *. 1e-9 in
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
    let ops_per_sec = nb_entriesf /. time_sec in
    let mbs_per_sec = entry_sizef *. nb_entriesf /. 1_048_576. /. time_sec in
    let replace_durations = stats.replace_durations in
    let merges_duration = List.fold_left Float.add 0. stats.merge_durations in
    let nb_merges = stats.nb_merge in
    {
      time;
      ops_per_sec;
      mbs_per_sec;
      read_amplification_calls;
      read_amplification_size;
      write_amplification_calls;
      write_amplification_size;
      replace_durations;
      merges_duration;
      nb_merges;
    }

  let pp_result fmt result =
    Format.fprintf fmt
      "@[<v 0>Total time: %a@,\
       Operations per second: %f@,\
       Mbytes per second: %f@,\
       Read amplification in syscalls: %f@,\
       Read amplification in bytes: %f@,\
       Write amplification in syscalls: %f@,\
       Write amplification in bytes: %f@,\
       Last 10 merges cumulated duration (μs): %f@\n\
       Number of merges : %d@]" Mtime.Span.pp result.time result.ops_per_sec
      result.mbs_per_sec result.read_amplification_calls
      result.read_amplification_size result.write_amplification_calls
      result.write_amplification_size result.merges_duration result.nb_merges
end

let make_bindings_pool nb_entries =
  Array.init nb_entries (fun _ ->
      let k = Context.Key.v () in
      let v = Context.Value.v () in
      (k, v))

let bindings_pool = ref [||]
let absent_bindings_pool = ref [||]
let sorted_bindings_pool = ref [||]

module Index_lib = Index

module Index = struct
  module Index =
    Index_unix.Private.Make (Context.Key) (Context.Value) (Index.Cache.Noop)

  let add_metrics =
    let no_tags x = x in
    fun () -> Metrics.add src no_tags (fun m -> m (Stats.get ()))

  let write ~with_metrics ?(with_flush = false) ?sampling_interval bindings rw =
    Array.iter
      (fun (k, v) ->
        Index.replace_with_timer ?sampling_interval rw k v;
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

  let write_random_with_timer ~with_metrics t () =
    write ~with_metrics ~sampling_interval:!replace_sampling_interval
      !bindings_pool t

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
    let index =
      Index.v ~cache:(Index.empty_cache ()) ~fresh ~readonly ~log_size
        (root // name)
    in
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
    speed : [ `Quick | `Slow ];
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
        speed = `Quick;
      };
      {
        name = "replace_random_with_timer";
        synopsis = "Replace in random order and time each operation";
        readonly = false;
        fresh = true;
        benchmark = write_random_with_timer;
        dependency = None;
        speed = `Slow;
      };
      {
        name = "replace_random_sync";
        synopsis = "Replace in random order with sync";
        readonly = false;
        fresh = true;
        benchmark = write_sync;
        dependency = None;
        speed = `Slow;
      };
      {
        name = "replace_increasing_keys";
        synopsis = "Replace in increasing order of keys";
        readonly = false;
        fresh = true;
        benchmark = write_seq;
        dependency = None;
        speed = `Slow;
      };
      {
        name = "replace_increasing_hash";
        synopsis = "Replace in increasing order of hashes";
        readonly = false;
        fresh = true;
        benchmark = write_seq_hash;
        dependency = None;
        speed = `Slow;
      };
      {
        name = "replace_decreasing_hash";
        synopsis = "Replace in decreasing order of hashes";
        readonly = false;
        fresh = true;
        benchmark = write_rev_seq_hash;
        dependency = None;
        speed = `Slow;
      };
      {
        name = "iter_rw";
        synopsis = "[RW] Iter";
        readonly = false;
        fresh = false;
        benchmark = iter;
        dependency = Some "replace_random";
        speed = `Slow;
      };
      {
        name = "find_random_ro";
        synopsis = "[RO] Find in random order";
        readonly = true;
        fresh = false;
        benchmark = find_random;
        dependency = Some "replace_random";
        speed = `Quick;
      };
      {
        name = "find_random_rw";
        synopsis = "[RW] Find in random order";
        readonly = false;
        fresh = false;
        benchmark = find_random;
        dependency = Some "replace_random";
        speed = `Quick;
      };
      {
        name = "find_absent_ro";
        synopsis = "[RO] Find absent values";
        readonly = true;
        fresh = false;
        benchmark = find_absent;
        dependency = Some "replace_random";
        speed = `Slow;
      };
      {
        name = "find_absent_rw";
        synopsis = "[RW] Find absent values";
        readonly = false;
        fresh = false;
        benchmark = find_absent;
        dependency = Some "replace_random";
        speed = `Slow;
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
  sampling_interval : int;
  minimal_flag : bool;
}
[@@deriving yojson]

let pp_config fmt config =
  Format.fprintf fmt
    "@[<v 0>Key size: %d@,\
     Value size: %d@,\
     Number of bindings: %d@,\
     Log size: %d@,\
     Seed: %d@,\
     Metrics: %b@,\
     Sampling interval: %d@]" config.key_size config.value_size
    config.nb_entries config.log_size config.seed config.with_metrics
    config.sampling_interval

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
    Metrics_unix.monitor_gc 0.1);
  bindings_pool := make_bindings_pool config.nb_entries;
  if not config.minimal_flag then (
    absent_bindings_pool := make_bindings_pool config.nb_entries;
    sorted_bindings_pool := Array.copy !bindings_pool;
    replace_sampling_interval := config.sampling_interval)

let print fmt (config, results) =
  let pp_bench fmt (b, result) =
    Format.fprintf fmt "@[<v 4>%s@,%a@]" b.Index.synopsis Benchmark.pp_result
      result
  in
  Format.fprintf fmt "@[<v 4>Configuration:@,%a@,@]@,@[<v 4>Results:@,%a@]@."
    pp_config config
    Fmt.(list ~sep:(any "@,@,") pp_bench)
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

let get_suite_list minimal_flag =
  if minimal_flag then
    List.filter (fun bench -> bench.Index.speed = `Quick) Index.suite
  else Index.suite

let repeat n f l =
  let rec aux i acc = if i = n then acc else aux (i + 1) (f l :: acc) in
  aux 0 []

let mean l =
  match l with
  | [ hd ] -> hd
  | hd :: tl ->
      let ll = List.length l in
      let ll_f = float ll in
      List.fold_left
        (fun acc bresult ->
          List.fold_left2
            (fun acc (tsm, resultm) (ts, result) ->
              assert (Index.(tsm.name = ts.name));
              ( tsm,
                Benchmark.
                  {
                    time = Mtime.Span.add resultm.time result.time;
                    ops_per_sec = resultm.ops_per_sec +. result.ops_per_sec;
                    mbs_per_sec = resultm.mbs_per_sec +. result.mbs_per_sec;
                    read_amplification_calls =
                      resultm.read_amplification_calls
                      +. result.read_amplification_calls;
                    read_amplification_size =
                      resultm.read_amplification_size
                      +. result.read_amplification_size;
                    write_amplification_calls =
                      resultm.write_amplification_calls
                      +. result.write_amplification_calls;
                    write_amplification_size =
                      resultm.write_amplification_size
                      +. result.write_amplification_size;
                    replace_durations =
                      List.map2 ( +. ) resultm.replace_durations
                        result.replace_durations;
                    merges_duration =
                      resultm.merges_duration +. result.merges_duration;
                    nb_merges = resultm.nb_merges + result.nb_merges;
                  } )
              :: acc)
            [] acc bresult
          |> List.rev)
        hd tl
      |> List.map (fun (es, res) ->
             ( es,
               Benchmark.
                 {
                   time = Mtime.Span.div res.time ll;
                   ops_per_sec = res.ops_per_sec /. ll_f;
                   mbs_per_sec = res.mbs_per_sec /. ll_f;
                   read_amplification_calls =
                     res.read_amplification_calls /. ll_f;
                   read_amplification_size = res.read_amplification_size /. ll_f;
                   write_amplification_calls =
                     res.write_amplification_calls /. ll_f;
                   write_amplification_size =
                     res.write_amplification_size /. ll_f;
                   replace_durations =
                     List.map (fun d -> d /. ll_f) res.replace_durations;
                   merges_duration = res.merges_duration /. ll_f;
                   nb_merges = res.nb_merges / ll;
                 } ))
  | _ -> assert false

let run filter root output seed with_metrics log_size nb_entries nb_exec json
    sampling_interval minimal_flag =
  let config =
    {
      key_size;
      value_size;
      nb_entries;
      log_size;
      seed;
      with_metrics;
      sampling_interval;
      minimal_flag;
    }
  in
  cleanup root;
  init config;
  let current_suite = get_suite_list config.minimal_flag in
  let name_filter =
    match filter with None -> fun _ -> true | Some re -> Re.execp re
  in
  current_suite
  |> schedule name_filter
  |> repeat nb_exec
       (List.map (fun (b : Index.suite_elt) ->
            let name =
              match b.dependency with None -> b.name | Some name -> name
            in
            let result =
              Index.run ~with_metrics ~nb_entries ~log_size ~root ~name
                ~fresh:b.fresh ~readonly:b.readonly b.benchmark
            in
            (b, result)))
  |> mean
  |> fun results ->
  let fmt =
    (match output with None -> stdout | Some filename -> open_out filename)
    |> Format.formatter_of_out_channel
  in
  Fmt.pf fmt "%a@." (if json then print_json else print) (config, results)

open Cmdliner

let deprecated_info = (Term.info [@alert "-deprecated"])
let deprecated_exit = (Term.exit [@alert "-deprecated"])
let deprecated_eval_choice = (Term.eval_choice [@alert "-deprecated"])
let deprecated_pure = (Term.pure [@alert "-deprecated"])
let deprecated_default_exits = (Term.default_exits [@alert "-deprecated"])
let deprecated_env_var = (Arg.env_var [@alert "-deprecated"])
let env_var s = deprecated_env_var ("INDEX_BENCH_" ^ s)

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

let nb_exec =
  let doc = "Number of times the benchmarks should be repeated" in
  let env = env_var "REP" in
  Arg.(value & opt int 1 & info [ "nb-exec" ] ~env ~doc)

let list_cmd =
  let doc = "List all available benchmarks." in
  (Term.(deprecated_pure list_benches $ const ()), deprecated_info "list" ~doc)

let json_flag =
  let doc = "Output the results as a json object." in
  let env = env_var "JSON" in
  Arg.(value & flag & info [ "j"; "json" ] ~env ~doc)

let sampling_interval =
  let doc = "Sampling interval for the duration of replace operations." in
  let env = env_var "REPLACE_DURATION_SAMPLING_INTERVAL" in
  Arg.(value & opt int 10 & info [ "sampling-interval" ] ~env ~doc)

let minimal_flag =
  let doc = "Run a set of minimal benchmarks" in
  let env = env_var "MINIMAL" in
  Arg.(value & flag & info [ "minimal" ] ~env ~doc)

let cmd =
  let doc = "Run all the benchmarks." in
  ( Term.(
      const (fun () -> run)
      $ Index_lib.Private.Logs.setup_term (module Mtime_clock)
      $ name_filter
      $ data_dir
      $ output
      $ seed
      $ metrics_flag
      $ log_size
      $ nb_entries
      $ nb_exec
      $ json_flag
      $ sampling_interval
      $ minimal_flag),
    deprecated_info "run" ~doc ~exits:deprecated_default_exits )

let () =
  let choices = [ list_cmd ] in
  deprecated_exit @@ deprecated_eval_choice cmd choices
