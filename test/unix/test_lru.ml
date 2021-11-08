(** Tests of the in-memory LRU used by the Index implementation.

    NOTE: most other tests in the suite set an LRU size of 0 for simplicity. *)

module Stats = Index.Stats
open Common

module Context = Common.Make_context (struct
  let root = Filename.concat "_tests" "test_log_with_lru"
end)

let check_bool pos ~expected act = Alcotest.(check ~pos bool) "" expected act
let check_int pos ~expected act = Alcotest.(check ~pos int) "" expected act

let check_value pos ~expected act =
  let key = Alcotest.testable Key.pp Key.equal in
  Alcotest.(check ~pos key) "" expected act

let check_lru_stats pos ~hits ~misses =
  Alcotest.(check ~pos int) "LRU hits" hits Stats.((get ()).lru_hits);
  Alcotest.(check ~pos int) "LRU misses" misses Stats.((get ()).lru_misses)

let get_new_cached_binding idx =
  let k, v = (Key.v (), Value.v ()) in
  Index.replace idx k v;
  check_value __POS__ ~expected:v (Index.find idx k);
  (k, v)

let test_replace_and_find () =
  let lru_size = 1 in
  let* { rw = idx; _ } = Context.with_empty_index ~lru_size () in

  (* Check that [replace] populates the LRU: *)
  let k1, v1 = (Key.v (), Value.v ()) in
  let () =
    Stats.reset_stats ();
    Index.replace idx k1 v1;
    (* [k1] is now in the LRU: *)
    check_value __POS__ ~expected:v1 (Index.find idx k1);
    check_lru_stats __POS__ ~hits:1 ~misses:0
  in

  (* Check that a second [replace] updates the LRU contents: *)
  let k2, v2 = (Key.v (), Value.v ()) in
  let () =
    assert (k1 <> k2);
    Stats.reset_stats ();
    Index.replace idx k2 v2;
    (* [k2] has replaced [k1] in the LRU, so we miss on [find k1]: *)
    check_value __POS__ ~expected:v1 (Index.find idx k1);
    check_lru_stats __POS__ ~hits:0 ~misses:1;
    (* [k1] is now in the LRU: *)
    check_value __POS__ ~expected:v1 (Index.find idx k1);
    check_lru_stats __POS__ ~hits:1 ~misses:1
  in
  ()

let test_mem () =
  let lru_size = 1 in
  let* { rw = idx; _ } = Context.with_empty_index ~lru_size () in

  (* Initially, [k2] is in the LRU and [k1] is not: *)
  let k1, k2, v1, v2 = (Key.v (), Key.v (), Value.v (), Value.v ()) in
  Index.replace idx k1 v1;
  Index.replace idx k2 v2;

  (* [mem k2] hits in the LRU: *)
  let () =
    Stats.reset_stats ();
    check_bool __POS__ ~expected:true (Index.mem idx k2);
    check_lru_stats __POS__ ~hits:1 ~misses:0
  in

  (* [mem k1] initially misses in the LRU, but subsequent calls hit in the LRU
     (because the [k2] binding is replaced by [k1] on the miss). *)
  let () =
    Stats.reset_stats ();
    check_bool __POS__ ~expected:true (Index.mem idx k1);
    check_lru_stats __POS__ ~hits:0 ~misses:1;
    check_bool __POS__ ~expected:true (Index.mem idx k1);
    check_lru_stats __POS__ ~hits:1 ~misses:1
  in
  ()

(* Check that the LRU is cleared on [clear]. *)
let test_clear () =
  let lru_size = 1 in
  let* { rw = idx; _ } = Context.with_full_index ~lru_size () in

  (* Add a binding and ensure that it's in the LRU: *)
  let k, v = (Key.v (), Value.v ()) in
  Index.replace idx k v;
  check_value __POS__ ~expected:v (Index.find idx k);

  (* We should miss in the LRU when attempting to find [k] after [clear]: *)
  Index.clear idx;
  Stats.reset_stats ();
  Alcotest.check_raises "find after clear" Not_found (fun () ->
      ignore (Index.find idx k));
  check_lru_stats __POS__ ~hits:0 ~misses:1

(* Check that bindings in the LRU are properly removed by [filter]: *)
let test_filter () =
  let lru_size = 1 in
  let* { rw = idx; _ } = Context.with_full_index ~lru_size () in

  (* Add a binding and ensure that it's in the LRU: *)
  let k, v = (Key.v (), Value.v ()) in
  Index.replace idx k v;
  check_value __POS__ ~expected:v (Index.find idx k);

  (* Remove [k] from the index via [filter], then try to [find] it: *)
  Index.filter idx (fun (k', _) -> not (Key.equal k k'));
  Stats.reset_stats ();
  Alcotest.check_raises ~pos:__POS__ "find after filter-false" Not_found
    (fun () -> ignore (Index.find idx k));
  check_lru_stats __POS__ ~hits:0 ~misses:1

let tests =
  [
    ("replace_and_find", `Quick, test_replace_and_find);
    ("mem", `Quick, test_mem);
    ("clear", `Quick, test_clear);
    ("filter", `Quick, test_filter);
  ]
