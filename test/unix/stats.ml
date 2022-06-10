module I = Index
open Common

let root = Filename.concat "_tests" "unix.stats"

module Context = Common.Make_context (struct
  let root = root
end)

module Stats = I.Stats
module Io_stats = Index.Io_stats
module Raw = Index_unix.Private.Raw
module Raw_stats = Index_unix.Private.Raw_stats

let test_replace () =
  let* Context.{ rw; _ } = Context.with_empty_index () in
  Stats.reset ();
  Io_stats.reset_all ();
  (* if reset_raw then Stats_raw.reset_stats (); *)
  let k1, v1, k2, v2, k3, v3 =
    (Key.v (), Value.v (), Key.v (), Value.v (), Key.v (), Value.v ())
  in
  Index.replace rw k1 v1;
  Index.replace rw k2 v2;
  Index.replace rw k3 v3;
  Index.flush rw;
  Alcotest.(check int) "no bytes read" 0 (Io_stats.bytes_read ());
  Alcotest.(check int) "no reads" 0 (Io_stats.nb_reads ());
  Alcotest.(check int)
    "one write for entries and a separate write for offset" 2
    (Io_stats.nb_writes ());
  Alcotest.(check int)
    "bytes written for 3 entries and an offset" 128
    (Io_stats.bytes_written ())

let test_find () =
  let* Context.{ rw; _ } = Context.with_empty_index () in
  let k1, v1 = (Key.v (), Value.v ()) in
  Index.replace rw k1 v1;
  Index.try_merge_aux ~force:true rw |> Index.await |> check_completed;
  Io_stats.reset_all ();
  ignore (Index.mem rw k1);
  Alcotest.(check int) "bytes read" 40 (Io_stats.bytes_read ());
  Alcotest.(check int) "no reads" 1 (Io_stats.nb_reads ());
  Alcotest.(check int) "no writes" 0 (Io_stats.nb_writes ());
  Alcotest.(check int) "no bytes written" 0 (Io_stats.bytes_written ())

let test_multiple_raw () =
  let test_single_raw = test_replace in
  let tmp = Filename.concat root "_tmp" in
  let x = Unix.openfile tmp Unix.[ O_CREAT; O_RDWR; O_CLOEXEC ] 0o644 in
  let raw = Raw.v x in
  Raw.Offset.set raw Optint.Int63.zero;
  test_single_raw ();
  let stats = Raw.get_stats raw in
  Alcotest.(check int) "no bytes read" stats.bytes_read 0;
  Alcotest.(check int) "no reads" stats.nb_reads 0;
  Alcotest.(check int) "one write for offset" stats.nb_writes 1;
  Alcotest.(check int) "bytes written for offset" stats.bytes_written 8;
  Unix.close x

let tests =
  [
    ("stats for replace", `Quick, test_replace);
    ("stats for find", `Quick, test_find);
    ("stats for different raw files", `Quick, test_multiple_raw);
  ]
