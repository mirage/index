(** Benchmarks *)

open Common
open Rresult

let src = Logs.Src.create "db_bench"

module Log = (val Logs.src_log src : Logs.LOG)

let all = ref false

and with_lmdb = ref false

and with_index = ref false

let speclist =
  [
    ("-all", Arg.Set all, "run all benchmarks");
    ("-lmdb", Arg.Set with_lmdb, "run the lmdb benchmarks");
    ("-index", Arg.Set with_index, "run the index benchmarks");
  ]

let usage =
  "usage: " ^ Sys.argv.(0)
  ^ " [-all | -lmdb | -index], if no argument provided then minimal index \
     benchmarks are run"

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

let no_tags x = x

let seed = 1

let () = Random.init seed

let key_size = 32

let hash_size = 30

let value_size = 13

let nb_entries = 10_000_000

let log_size = 500_000

let ( // ) = Filename.concat

module Context = Make_context (struct
  let key_size = key_size

  let hash_size = hash_size

  let value_size = value_size
end)

let entry_size = key_size + value_size

let random = Array.make nb_entries ("", "")

let populate () =
  let rec loop i =
    if i = nb_entries then ()
    else
      let k = Context.Key.v () in
      let v = Context.Value.v () in
      let () = random.(i) <- (k, v) in
      loop (i + 1)
  in
  loop 0

let print_results db f =
  let _, time = with_timer f in
  let micros = time *. 1_000_000. in
  let sec_op = micros /. float_of_int nb_entries in
  let mb = float_of_int (entry_size * nb_entries / 1_000_000) /. time in
  let ops_sec = float_of_int nb_entries /. time in
  Log.app (fun l ->
      l "%s: %f micros/op; \t %f op/s; \t %f MB/s; \t total time = %fs." db
        sec_op ops_sec mb time);
  time

module Index = struct
  module Index = Index_unix.Make (Context.Key) (Context.Value)

  let root = "_bench" // "db_bench"

  let print_results = print_results "index"

  let write_amplif ?exec_time () =
    let stats = Stats.get () in
    let ratio_bytes =
      float_of_int stats.bytes_written /. float_of_int (entry_size * nb_entries)
    in
    let ratio_reads =
      float_of_int stats.nb_writes /. float_of_int nb_entries
    in
    Log.app (fun l ->
        l "\twrite amplification in bytes = %f; in nb of writes = %f; "
          ratio_bytes ratio_reads);
    match exec_time with
    | None -> ()
    | Some exec_time ->
        let ratio_merge = stats.merge_time /. exec_time in
        Log.app (fun l ->
            l "\tmerge time = %f; ratio = %f; " stats.merge_time ratio_merge)

  let read_amplif () =
    let stats = Stats.get () in
    let ratio_bytes =
      float_of_int stats.bytes_read /. float_of_int (entry_size * nb_entries)
    in
    let ratio_reads = float_of_int stats.nb_reads /. float_of_int nb_entries in
    Log.app (fun l ->
        l "\tread amplification in bytes = %f; in nb of reads = %f "
          ratio_bytes ratio_reads)

  let init () =
    if Sys.file_exists root then (
      let cmd = Printf.sprintf "rm -rf %s" root in
      Logs.app (fun l -> l "exec: %s\n" cmd);
      let _ = Sys.command cmd in
      () )

  let write ~with_metrics rw () =
    Array.iter
      (fun (k, v) ->
        Index.replace rw k v;
        if with_metrics then
          Metrics.add src no_tags (fun m -> m (Stats.get ())))
      random

  let read ~with_metrics r () =
    Array.iter
      (fun (k, _) ->
        ignore (Index.find r k);
        if with_metrics then
          Metrics.add src no_tags (fun m -> m (Stats.get ())))
      random

  let write_random () =
    Log.debug (fun l -> l "write_radom");
    Stats.reset_stats ();
    let rw = Index.v ~fresh:true ~log_size (root // "fill_random") in
    let exec_time = print_results (write ~with_metrics:false rw) in
    write_amplif ~exec_time ();
    rw

  let write_seq () =
    Stats.reset_stats ();
    let rw = Index.v ~fresh:true ~log_size (root // "fill_seq") in
    Array.sort (fun a b -> String.compare (fst a) (fst b)) random;
    ignore (print_results (write ~with_metrics:false rw));
    write_amplif ();
    Index.close rw

  let write_seq_hash () =
    Stats.reset_stats ();
    let rw = Index.v ~fresh:true ~log_size (root // "fill_seq_hash") in
    let hash e = Context.Key.hash (fst e) in
    Array.sort (fun a b -> compare (hash a) (hash b)) random;
    ignore (print_results (write ~with_metrics:false rw));
    write_amplif ();
    Index.close rw

  let write_rev_seq_hash () =
    Stats.reset_stats ();
    let rw = Index.v ~fresh:true ~log_size (root // "fill_rev_seq_hash") in
    let hash e = Context.Key.hash (fst e) in
    Array.sort (fun a b -> compare (hash a) (hash b)) random;
    let write rw =
      Array.fold_right (fun (k, v) () -> Index.replace rw k v) random
    in
    ignore (print_results (write rw));
    write_amplif ();
    Index.close rw

  let write_sync () =
    Stats.reset_stats ();
    let rw = Index.v ~fresh:true ~log_size (root // "fill_sync") in
    let write rw () =
      Array.iter
        (fun (k, v) ->
          Index.replace rw k v;
          Index.flush rw)
        random
    in
    ignore (print_results (write rw));
    write_amplif ();
    Index.close rw

  let overwrite rw =
    Stats.reset_stats ();
    ignore (print_results (write ~with_metrics:false rw));
    write_amplif ()

  let read_random r =
    Stats.reset_stats ();
    ignore (print_results (read ~with_metrics:false r));
    read_amplif ()

  let read_seq r =
    Stats.reset_stats ();
    let read () = Index.iter (fun _ _ -> ()) r in
    ignore (print_results read);
    read_amplif ()

  let close rw = Index.close rw
end

module Lmdb = struct
  open Lmdb

  let root = "/tmp"

  let print_results = print_results "lmdb "

  let print_stats (txn, ddb) =
    let stats = R.get_ok (db_stat txn ddb) in
    Log.app (fun l ->
        l
          "psize = %d; depth= %d; branch_pages= %d; leaf_pages= %d; \
           overflow_pages= %d; entries= %d;"
          stats.psize stats.depth stats.branch_pages stats.leaf_pages
          stats.overflow_pages stats.entries)

  let cleanup () =
    let files = [ root // "data.mdb"; root // "lock.mdb" ] in
    ListLabels.iter files ~f:(fun fn -> Sys.(if file_exists fn then remove fn))

  let fail_on_error f =
    match f () with Ok _ -> () | Error err -> failwith (string_of_error err)

  let flags = [ Lmdb.NoRdAhead; Lmdb.NoSync; Lmdb.NoMetaSync; Lmdb.NoTLS ]

  let mapsize = 409_600_000_000L

  let get_wtxn dir flags =
    cleanup ();
    opendir dir ~mapsize ~flags 0o644 >>= fun env ->
    create_rw_txn env >>= fun txn ->
    opendb txn >>= fun ddb -> Ok ((txn, ddb), env)

  let write (txn, ddb) () =
    Array.iter
      (fun (k, v) -> fail_on_error (fun () -> Lmdb.put_string txn ddb k v))
      random

  let read (txn, ddb) () =
    Array.iter
      (fun (k, _) ->
        ignore (Bigstring.to_string (R.get_ok (Lmdb.get txn ddb k))))
      random

  let write_random () =
    get_wtxn root flags >>| fun (rw, env) ->
    ignore (print_results (write rw));
    print_stats rw;
    (rw, env)

  let write_seq () =
    Array.sort (fun a b -> String.compare (fst a) (fst b)) random;
    get_wtxn root flags >>| fun (rw, env) ->
    ignore (print_results (write rw));
    closedir env

  let write_sync () =
    get_wtxn root [ Lmdb.NoRdAhead ] >>| fun (rw, env) ->
    let write (txn, ddb) env ls () =
      Array.iter
        (fun (k, v) ->
          fail_on_error (fun () ->
              Lmdb.put_string txn ddb k v >>= fun () -> sync env))
        ls
    in
    ignore (print_results (write rw env random));
    closedir env

  let overwrite rw = ignore (print_results (write rw))

  let read_random r = ignore (print_results (read r))

  (*use a new db, created without the flag Lmdb.NoRdAhead*)
  let read_seq () =
    let rw, env =
      R.get_ok
        ( get_wtxn root [ Lmdb.NoSync; Lmdb.NoMetaSync ] >>| fun (rw, env) ->
          let () = write rw () in
          (rw, env) )
    in
    let read (txn, ddb) () =
      opencursor txn ddb >>= fun cursor ->
      cursor_first cursor >>= fun () ->
      cursor_iter
        ~f:(fun (k, v) ->
          ignore (Bigstring.to_string k);
          ignore (Bigstring.to_string v);
          Ok ())
        cursor
      >>| fun () -> cursor_close cursor
    in
    let aux_read r () = fail_on_error (read r) in
    ignore (print_results (aux_read rw));
    closedir env

  let close env = closedir env
end

let init () =
  Common.report ();
  Index.init ();
  Lmdb.cleanup ();
  Log.app (fun l -> l "Keys: %d bytes each." key_size);
  Log.app (fun l -> l "Values: %d bytes each." value_size);
  Log.app (fun l -> l "Entries: %d." nb_entries);
  Log.app (fun l -> l "Log size: %d." log_size);
  populate ()

let index_main () =
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in increasing order of keys");
  Index.write_seq ();
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in increasing order of hashes");
  Index.write_seq_hash ();
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in decreasing order of hashes");
  Index.write_rev_seq_hash ();
  populate ();
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in random order and sync after each write");
  Index.write_sync ();
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in random order");
  let rw = Index.write_random () in
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Read in random order ");
  Index.read_random rw;
  Log.app (fun l -> l "\n");
  Log.app (fun l ->
      l
        "Read in sequential order (increasing order of hashes for index, \
         increasing order of keys for lmdb)");
  Index.read_seq rw;
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Overwrite");
  Index.overwrite rw;
  Index.close rw

let lmdb_main () =
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in increasing order of keys");
  Lmdb.fail_on_error Lmdb.write_seq;
  populate ();
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in random order and sync after each write");
  Lmdb.fail_on_error Lmdb.write_sync;
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in random order");
  let lmdb, env = R.get_ok (Lmdb.write_random ()) in
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Read in random order ");
  Lmdb.read_random lmdb;
  Log.app (fun l -> l "\n");
  Log.app (fun l ->
      l
        "Read in sequential order (increasing order of hashes for index, \
         increasing order of keys for lmdb)");
  Lmdb.read_seq ();
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Overwrite");
  Lmdb.overwrite lmdb;
  Lmdb.close env

let minimal_benchs () =
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in random order");
  let rw = Index.write_random () in
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Read in random order ");
  Index.read_random rw;
  Log.app (fun l -> l "\n");
  Log.app (fun l ->
      l "Read in sequential order (increasing order of hashes for index)");
  Index.read_seq rw;
  Index.close rw

let () =
  Arg.parse speclist ignore usage;
  Metrics.enable_all ();
  Metrics_gnuplot.set_reporter ();
  Metrics_unix.monitor_gc 0.1;
  init ();
  if !all || !with_index then index_main ();
  if !all || !with_lmdb then lmdb_main ();
  if not (!all || !with_lmdb || !with_index) then minimal_benchs ()
