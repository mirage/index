(** Benchmarks *)

open Common
open Rresult

let src = Logs.Src.create "db_bench"

module Log = (val Logs.src_log src : Logs.LOG)

let seed = 1

let () = Random.init seed

let key_size = 44

(* the hash_size in pack_index*)
let hash_size = 60

let value_size = 13

let nb_entries = 1_000_000

(* in the tezos use case, log_size is proportional to the nb_entries *)
let log_size = nb_entries / 5

let ( // ) = Filename.concat

module Context = Make_context (struct
  let key_size = key_size

  let hash_size = hash_size

  let value_size = value_size
end)

let entry_size = key_size + value_size

let random =
  let rec loop i acc =
    if i = nb_entries then acc
    else
      let k = Context.Key.v () in
      let v = Context.Value.v () in
      loop (i + 1) ((k, v) :: acc)
  in
  loop 0 []

let sorted = List.sort (fun a b -> String.compare (fst a) (fst b)) random

let print_results db f nb_entries =
  let _, time = with_timer f in
  let micros = time *. 1_000_000. in
  let op = micros /. float_of_int nb_entries in
  let mb = float_of_int (entry_size * nb_entries / 1_000_000) /. time in
  Log.info (fun l ->
      l "%s: %f micros/op; \t %f MB/s \t total time = %fs.%!" db op mb time)

let write_amplif offset nb_entries =
  let ratio = float_of_int offset /. float_of_int (entry_size * nb_entries) in
  Log.info (fun l -> l "\t\twrite amplification = %f%!" ratio)

let get_offset file =
  if Sys.file_exists file then (
    let fd = Unix.openfile file Unix.[ O_RDONLY ] 0o644 in
    let offset = Unix.lseek fd 0 Unix.SEEK_END in
    Unix.close fd;
    offset )
  else 0

module Index = struct
  module Index = Index_unix.Make (Context.Key) (Context.Value)

  let root = "_bench" // "db_bench"

  let print_results = print_results "index"

  let init () =
    if Sys.file_exists root then (
      let cmd = Printf.sprintf "rm -rf %s" root in
      Logs.info (fun l -> l "exec: %s\n%!" cmd);
      let _ = Sys.command cmd in
      () )

  let file_size dir =
    let log_offset = get_offset (dir // "index" // "log") in
    let data_offset = get_offset (dir // "index" // "data") in
    log_offset + data_offset

  let write rw ls () = List.iter (fun (k, v) -> Index.replace rw k v) ls

  let read r ls () = List.iter (fun (k, _) -> ignore (Index.find r k)) ls

  let write_random () =
    let rw = Index.v ~fresh:true ~log_size (root // "fill_random") in
    print_results (write rw random) nb_entries;

    (*Index.flush rw;*)
    write_amplif (file_size (root // "fill_random")) nb_entries;
    rw

  let write_seq () =
    let rw = Index.v ~fresh:true ~log_size (root // "fill_seq") in
    print_results (write rw sorted) nb_entries;

    (*Index.flush rw;*)
    write_amplif (file_size (root // "fill_seq")) nb_entries;
    Index.close rw

  let write_seq_hash () =
    let rw = Index.v ~fresh:true ~log_size (root // "fill_seq_hash") in
    let hash e = Context.Key.hash (fst e) in
    let sorted = List.sort (fun a b -> compare (hash a) (hash b)) random in
    print_results (write rw sorted) nb_entries;

    (*Index.flush rw;*)
    write_amplif (file_size (root // "fill_seq_hash")) nb_entries;
    Index.close rw

  let write_sync () =
    let rw = Index.v ~fresh:true ~log_size (root // "fill_sync") in
    let write rw () =
      List.iter
        (fun (k, v) ->
          Index.replace rw k v;
          Index.flush rw)
        random
    in
    print_results (write rw) nb_entries;
    write_amplif (file_size (root // "fill_sync")) nb_entries;
    Index.close rw

  let overwrite rw =
    let sorted_uniq =
      List.sort_uniq (fun a b -> compare (fst a) (fst b)) random
    in
    let nb_entries = List.length sorted_uniq in
    print_results (write rw sorted_uniq) nb_entries;
    write_amplif (file_size (root // "fill_random")) nb_entries

  let read_random r = print_results (read r random) nb_entries

  let read_seq r = print_results (read r sorted) nb_entries

  let read_seq_rev r = print_results (read r (List.rev sorted)) nb_entries

  let close rw = Index.close rw
end

module Lmdb = struct
  open Lmdb

  let root = "/tmp"

  let print_results = print_results "lmdb "

  let cleanup () =
    let files = [ root // "data.mdb"; root // "lock.mdb" ] in
    ListLabels.iter files ~f:(fun fn -> Sys.(if file_exists fn then remove fn))

  let file_size () =
    let offset = get_offset (root // "data.mdb") in
    if offset = 0 then failwith ("Error opening files in " ^ root ^ "/data.mdb");
    offset

  let fail_on_error f =
    match f () with Ok _ -> () | Error err -> failwith (string_of_error err)

  let flags = [ Lmdb.NoRdAhead; Lmdb.NoSync ]

  let mapsize = 409_600_000_000L

  let get_wtxn dir =
    cleanup ();
    opendir dir ~mapsize ~flags 0o644 >>= fun env ->
    create_rw_txn env >>= fun txn ->
    opendb txn >>= fun ddb -> Ok ((txn, ddb), env)

  let write (txn, ddb) ls () =
    List.iter
      (fun (k, v) -> fail_on_error (fun () -> Lmdb.put_string txn ddb k v))
      ls

  let read (txn, ddb) ls () =
    List.iter
      (fun (k, _) ->
        ignore (Bigstring.to_string (R.get_ok (Lmdb.get txn ddb k))))
      ls

  let write_random () =
    get_wtxn root >>| fun (rw, env) ->
    print_results (write rw random) nb_entries;
    write_amplif (file_size ()) nb_entries;
    (rw, env)

  let write_seq () =
    get_wtxn root >>| fun (rw, env) ->
    print_results (write rw sorted) nb_entries;
    write_amplif (file_size ()) nb_entries;
    closedir env

  let write_sync () =
    get_wtxn root >>| fun (rw, env) ->
    let write (txn, ddb) env ls () =
      List.iter
        (fun (k, v) ->
          fail_on_error (fun () ->
              Lmdb.put_string txn ddb k v >>= fun () -> sync env))
        ls
    in
    print_results (write rw env random) nb_entries;
    write_amplif (file_size ()) nb_entries;
    closedir env

  let overwrite rw =
    let sorted_uniq =
      List.sort_uniq (fun a b -> compare (fst a) (fst b)) random
    in
    let nb_entries = List.length sorted_uniq in
    print_results (write rw sorted_uniq) nb_entries;
    write_amplif (file_size ()) nb_entries

  let read_random r = print_results (read r random) nb_entries

  let read_seq r = print_results (read r sorted) nb_entries

  let read_seq_rev r = print_results (read r (List.rev sorted)) nb_entries

  let close env = closedir env
end

let main () =
  Common.report ();
  Index.init ();
  Lmdb.cleanup ();
  Log.info (fun l -> l "Keys: %d bytes each.%!" key_size);
  Log.info (fun l -> l "Values: %d bytes each.%!" value_size);
  Log.info (fun l -> l "Entries: %d.%!" nb_entries);
  Log.info (fun l -> l "Log size: %d.%!" log_size);

  Log.info (fun l -> l "\n%!");
  Log.info (fun l -> l "Fill in sequential order%!");
  Index.write_seq ();
  Lmdb.fail_on_error (fun () -> Lmdb.write_seq ());

  Log.info (fun l -> l "\n%!");
  Log.info (fun l -> l "Fill in sequential order of hashes%!");
  Index.write_seq_hash ();

  Log.info (fun l -> l "\n%!");
  Log.info (fun l -> l "Fill in random order and sync%!");
  Index.write_sync ();
  Lmdb.fail_on_error (fun () -> Lmdb.write_sync ());

  Log.info (fun l -> l "\n%!");
  Log.info (fun l -> l "Fill in random order %!");
  let rw = Index.write_random () in
  let lmdb, env = R.get_ok (Lmdb.write_random ()) in
  Log.info (fun l -> l "\n%!");
  Log.info (fun l -> l "Overwrite%!");
  Index.overwrite rw;
  Lmdb.overwrite lmdb;

  Log.info (fun l -> l "\n%!");
  Log.info (fun l -> l "Read in random order %!");
  Index.read_random rw;
  Lmdb.read_random lmdb;

  Log.info (fun l -> l "\n%!");
  Log.info (fun l -> l "Read in sequential order %!");
  Index.read_seq rw;
  Lmdb.read_seq lmdb;

  Log.info (fun l -> l "\n%!");
  Log.info (fun l -> l "Read in reverse sequential order %!");
  Index.read_seq_rev rw;
  Lmdb.read_seq_rev lmdb;

  Index.close rw;
  Lmdb.close env

let () = main ()
