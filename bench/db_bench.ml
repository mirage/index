(** Benchmarks *)

open Common
open Rresult

let src = Logs.Src.create "db_bench"

module Log = (val Logs.src_log src : Logs.LOG)

let seed = 1

let () = Random.init seed

let key_size = 32

let hash_size = 30

let value_size = 13

let nb_entries = 10_000_000

let log_size = 500_000

let ( // ) = Filename.concat

let root = ref "_bench"

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

let print_results db f nb_entries =
  let _, time = with_timer f in
  let micros = time *. 1_000_000. in
  let sec_op = micros /. float_of_int nb_entries in
  let mb = float_of_int (entry_size * nb_entries / 1_000_000) /. time in
  let ops_sec = float_of_int nb_entries /. time in
  Log.app (fun l ->
      l "%s: %f micros/op; \t %f op/s; \t %f MB/s; \t total time = %fs." db
        sec_op ops_sec mb time)

let rec random_new_key ar =
  let k = Context.Key.v () in
  if Array.exists (fun (k', _) -> k = k') ar then random_new_key ar else k

let populate_absents ar nb_entries =
  let absents = Array.make nb_entries ("", "") in
  let v = Context.Value.v () in
  let rec loop i =
    if i = nb_entries then absents
    else
      let k = random_new_key ar in
      let () = absents.(i) <- (k, v) in
      loop (i + 1)
  in
  loop 0

module Index = struct
  module Index = Index_unix.Make (Context.Key) (Context.Value)

  let print_results = print_results "index"

  let write_amplif () =
    let stats = Index_unix.get_stats () in
    let ratio_bytes =
      float_of_int stats.bytes_written /. float_of_int (entry_size * nb_entries)
    in
    let ratio_reads = float_of_int stats.nb_writes /. float_of_int nb_entries in
    Log.app (fun l ->
        l "\twrite amplification in bytes = %f; in nb of writes = %f; "
          ratio_bytes ratio_reads)

  let read_amplif nb_entries =
    let stats = Index_unix.get_stats () in
    let ratio_bytes =
      float_of_int stats.bytes_read /. float_of_int (entry_size * nb_entries)
    in
    let ratio_reads = float_of_int stats.nb_reads /. float_of_int nb_entries in
    Log.app (fun l ->
        l "\tread amplification in bytes = %f; in nb of reads = %f " ratio_bytes
          ratio_reads)

  let bench_list =
    [
      "fill_random";
      "fill_seq";
      "fill_seq_hash";
      "fill_rev_seq_hash";
      "fill_sync";
    ]

  let namespace_v ?(fresh = true) ?(readonly = false) bench_name =
    Index.v ~fresh ~log_size ~readonly (!root // bench_name)

  let write_bench ~bench bench_name =
    if List.mem bench_name bench_list then (
      Index_unix.reset_stats ();
      let rw = bench (namespace_v bench_name) in
      write_amplif ();
      rw )
    else failwith "Add the bench name to bench_list"

  let cleanup () =
    let files = [ "data"; "log"; "lock"; "log_async"; "merge" ] in
    List.iter
      (fun dir ->
        let dir = !root // dir // "index" in
        List.iter
          (fun file ->
            let file = dir // file in
            if Sys.file_exists file then (
              let cmd = Printf.sprintf "rm %s" file in
              Logs.app (fun l -> l "exec: %s\n" cmd);
              let _ = Sys.command cmd in
              () ))
          files)
      bench_list

  let write rw () = Array.iter (fun (k, v) -> Index.replace rw k v) random

  let read r () = Array.iter (fun (k, _) -> ignore (Index.find r k)) random

  let write_random () =
    let bench rw =
      print_results (write rw) nb_entries;
      rw
    in
    write_bench ~bench "fill_random"

  let write_seq () =
    let bench rw =
      Array.sort (fun a b -> String.compare (fst a) (fst b)) random;
      print_results (write rw) nb_entries;
      Index.close rw
    in
    ignore (write_bench ~bench "fill_seq")

  let write_seq_hash () =
    let bench rw =
      let hash e = Context.Key.hash (fst e) in
      Array.sort (fun a b -> compare (hash a) (hash b)) random;
      print_results (write rw) nb_entries;
      Index.close rw
    in
    ignore (write_bench ~bench "fill_seq_hash")

  let write_rev_seq_hash () =
    let bench rw =
      let hash e = Context.Key.hash (fst e) in
      Array.sort (fun a b -> compare (hash a) (hash b)) random;
      let write rw =
        Array.fold_right (fun (k, v) () -> Index.replace rw k v) random
      in
      print_results (write rw) nb_entries;
      Index.close rw
    in
    ignore (write_bench ~bench "fill_rev_seq_hash")

  let write_sync () =
    let bench rw =
      let write rw () =
        Array.iter
          (fun (k, v) ->
            Index.replace rw k v;
            Index.flush rw)
          random
      in
      print_results (write rw) nb_entries;
      Index.close rw
    in
    ignore (write_bench ~bench "fill_sync")

  let overwrite rw =
    Index_unix.reset_stats ();
    print_results (write rw) nb_entries;
    write_amplif ()

  let read_random r =
    Index_unix.reset_stats ();
    print_results (read r) nb_entries;
    read_amplif nb_entries

  let ro_read_random rw =
    Index.flush rw;
    Index_unix.reset_stats ();
    let ro = namespace_v ~fresh:false ~readonly:true "fill_random" in
    print_results (read ro) nb_entries;
    read_amplif nb_entries

  let read_seq r =
    Index_unix.reset_stats ();
    let read () = Index.iter (fun _ _ -> ()) r in
    print_results read nb_entries;
    read_amplif nb_entries

  let read_absent r =
    let absents = populate_absents random 1000 in
    let read r () =
      Array.iter
        (fun (k, _) -> try ignore (Index.find r k) with Not_found -> ())
        absents
    in
    Index_unix.reset_stats ();
    print_results (read r) 1000;
    read_amplif 1000

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
    print_results (write rw) nb_entries;
    print_stats rw;
    (rw, env)

  let write_seq () =
    Array.sort (fun a b -> String.compare (fst a) (fst b)) random;
    get_wtxn root flags >>| fun (rw, env) ->
    print_results (write rw) nb_entries;
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
    print_results (write rw env random) nb_entries;
    closedir env

  let overwrite rw = print_results (write rw) nb_entries

  let read_random r = print_results (read r) nb_entries

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
    print_results (aux_read rw) nb_entries;
    closedir env

  let close env = closedir env
end

let lmdb_benchmarks () =
  Log.app (fun l -> l "\n Fill in increasing order of keys");
  Lmdb.fail_on_error Lmdb.write_seq;
  populate ();
  Log.app (fun l -> l "\n Fill in random order and sync after each write");
  Lmdb.fail_on_error Lmdb.write_sync;
  Log.app (fun l -> l "\n Fill in random order");
  let lmdb, env = R.get_ok (Lmdb.write_random ()) in
  Log.app (fun l -> l "\n Read in random order ");
  Lmdb.read_random lmdb;
  Log.app (fun l -> l "\n Read in increasing order of keys");
  Lmdb.read_seq ();
  Log.app (fun l -> l "\n Overwrite");
  Lmdb.overwrite lmdb;
  Lmdb.close env

let init () =
  Common.report ();
  Index.cleanup ();
  Lmdb.cleanup ();
  Log.app (fun l -> l "Keys: %d bytes each." key_size);
  Log.app (fun l -> l "Values: %d bytes each." value_size);
  Log.app (fun l -> l "Entries: %d." nb_entries);
  Log.app (fun l -> l "Log size: %d." log_size);
  populate ()

let run input data_dir =
  root := data_dir // "db_bench";
  init ();
  Log.app (fun l -> l "\n");
  Log.app (fun l -> l "Fill in random order");
  let rw = Index.write_random () in
  let bench_list =
    [
      ( (fun () -> Index.read_random rw),
        [ `Read `RW; `All; `Minimal; `Index ],
        "RW Read in random order" );
      ( (fun () -> Index.ro_read_random rw),
        [ `Read `Ro; `All; `Minimal; `Index ],
        "RO Read in random order" );
      ( (fun () -> Index.read_absent rw),
        [ `Read `Absent; `All; `Minimal; `Index ],
        "Read 1000 absent values" );
      ( (fun () -> Index.read_seq rw),
        [ `Read `Seq; `All; `Index ],
        "Read in sequential order (increasing order of hashes for index" );
      ( Index.write_seq,
        [ `Write `IncKey; `All; `Index ],
        "Fill in increasing order of keys" );
      ( Index.write_seq_hash,
        [ `Write `IncHash; `All; `Index ],
        "Fill in increasing order of hashes" );
      ( Index.write_rev_seq_hash,
        [ `Write `DecHash; `All; `Index ],
        "Fill in decreasing order of hashes" );
      ( Index.write_sync,
        [ `Write `Sync; `All; `Index ],
        "Fill in random order and sync after each write" );
      ((fun () -> Index.overwrite rw), [ `OverWrite; `All; `Index ], "OverWrite");
      ((fun () -> lmdb_benchmarks ()), [ `Lmdb ], "Run lmdb benchmarks");
    ]
  in
  let match_input ~bench ~triggers ~message =
    if List.mem input triggers then
      let () = Log.app (fun l -> l "\n %s" message) in
      bench ()
    else ()
  in
  let () =
    List.iter
      (fun (bench, triggers, message) -> match_input ~bench ~triggers ~message)
      bench_list
  in
  Index.close rw

open Cmdliner

let input =
  let doc =
    "Select which benchmark(s) to run. Available options are: `write`, \
     `write-keys`, `write-hashes`, `write-dec`, `read-rw`, `read-ro` , \
     `read-seq`,  `read-absent`, `overwrite`, `minimal`, `index`, `lmdb` or \
     `all`. Default option is `minimal`"
  in
  let options =
    Arg.enum
      [
        ("all", `All);
        ("index", `Index);
        ("lmdb", `Lmdb);
        ("minimal", `Minimal);
        ("read-rw", `Read `RW);
        ("read-ro", `Read `RO);
        ("read-seq", `Read `Seq);
        ("read-absent", `Read `Absent);
        ("write-keys", `Write `IncKey);
        ("write-hashes", `Write `IncHash);
        ("write-dec", `Write `DecHash);
        ("write-sync", `Write `Sync);
        ("overwrite", `OverWrite);
      ]
  in
  Arg.(value & opt options `Minimal & info [ "b"; "bench" ] ~doc)

let data_dir =
  let doc = "Set directory for the data files" in
  Arg.(value & opt dir !root & info [ "d"; "directory" ] ~doc)

let cmd =
  let doc = "Specify the benchmark you want to run." in
  ( Term.(const run $ input $ data_dir),
    Term.info "run" ~doc ~exits:Term.default_exits )

let () = Term.(exit @@ eval cmd)
