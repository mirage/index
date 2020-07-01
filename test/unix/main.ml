module I = Index
open Common

let ( // ) = Filename.concat

let root = "_tests" // "unix.main"

module Context = Common.Make_context (struct
  let root = root
end)

let main = Context.full_index ()

(* Helper functions *)

(** [random_new_key tbl] returns a random key which is not in [tbl]. *)
let rec random_new_key tbl =
  let r = Key.v () in
  if Hashtbl.mem tbl r then random_new_key tbl else r

exception Found of string

(** [random_existing_key tbl] returns a random key from [tbl]. *)
let random_existing_key tbl =
  try
    Hashtbl.iter (fun k _ -> raise (Found k)) tbl;
    Alcotest.fail "Provided table contains no keys."
  with Found k -> k

let check_entry findf k v =
  match findf k with
  | v' when v = v' -> ()
  | v' (* v =/= v' *) ->
      Alcotest.failf "Wrong insertion: found %s when expected %s at key %s." v'
        v k
  | exception Not_found ->
      Alcotest.failf "Wrong insertion: %s key is missing." k

let check_index_entry index = check_entry (Index.find index)

let check_tbl_entry tbl = check_entry (Hashtbl.find tbl)

let check_equivalence index htbl =
  Hashtbl.iter (check_index_entry index) htbl;
  Index.iter (check_tbl_entry htbl) index

let test_replace t =
  let k = Key.v () in
  let v = Value.v () in
  let v' = Value.v () in
  Index.replace t k v;
  Index.replace t k v';
  check_index_entry t k v'

let test_find_absent t tbl =
  let rec loop i =
    if i = 0 then ()
    else
      let k = random_new_key tbl in
      Alcotest.check_raises (Printf.sprintf "Absent value was found: %s." k)
        Not_found (fun () -> ignore_value (Index.find t k));
      loop (i - 1)
  in
  loop 100

let mem_entry f k _ =
  if not (f k) then Alcotest.failf "Wrong insertion: %s key is missing." k

let mem_index_entry index = mem_entry (Index.mem index)

let mem_tbl_entry tbl = mem_entry (Hashtbl.mem tbl)

let check_equivalence_mem index tbl =
  Hashtbl.iter (mem_index_entry index) tbl;
  Index.iter (mem_tbl_entry tbl) index

(* Basic tests of find/replace on a live index *)
module Live = struct
  let find_present_live () =
    let Context.{ rw; tbl; _ } = Context.full_index () in
    check_equivalence rw tbl;
    Index.close rw

  let find_absent_live () =
    let Context.{ rw; tbl; _ } = Context.full_index () in
    test_find_absent rw tbl;
    Index.close rw

  let replace_live () =
    let Context.{ rw; _ } = Context.full_index () in
    test_replace rw;
    Index.close rw

  let different_size_for_key () =
    let Context.{ rw; _ } = Context.empty_index () in
    let k = String.init 2 (fun _i -> random_char ()) in
    let v = Value.v () in
    let exn = Index.Invalid_key_size k in
    Alcotest.check_raises
      "Cannot add a key of a different size than string_size." exn (fun () ->
        Index.replace rw k v);
    Index.close rw

  let different_size_for_value () =
    let Context.{ rw; _ } = Context.empty_index () in
    let k = Key.v () in
    let v = String.init 200 (fun _i -> random_char ()) in
    let exn = Index.Invalid_value_size v in
    Alcotest.check_raises
      "Cannot add a value of a different size than string_size." exn (fun () ->
        Index.replace rw k v);
    Index.close rw

  let membership () =
    let Context.{ rw; tbl; _ } = Context.full_index () in
    check_equivalence_mem rw tbl;
    Index.close rw

  let iter_after_clear () =
    let Context.{ rw; _ } = Context.full_index () in
    let () = Index.clear rw in
    Index.iter (fun _ _ -> Alcotest.fail "Indexed not cleared.") rw;
    Index.close rw

  let find_after_clear () =
    let Context.{ rw; tbl; _ } = Context.full_index () in
    let () = Index.clear rw in
    Hashtbl.fold
      (fun k _ () ->
        match Index.find rw k with
        | exception Not_found -> ()
        | _ -> Alcotest.fail "Indexed not cleared.")
      tbl ();
    Index.close rw

  let open_after_clear () =
    let Context.{ clone; rw; _ } = Context.full_index () in
    Index.clear rw;
    let rw2 = clone ~fresh:false ~readonly:false () in
    Alcotest.check_raises "Finding absent should raise Not_found" Not_found
      (fun () -> Key.v () |> Index.find rw2 |> ignore_value);
    Index.close rw2;
    Index.close rw

  let tests =
    [
      ("find (present)", `Quick, find_present_live);
      ("find (absent)", `Quick, find_absent_live);
      ("replace", `Quick, replace_live);
      ("fail add (key)", `Quick, different_size_for_key);
      ("fail add (value)", `Quick, different_size_for_value);
      ("membership", `Quick, membership);
      ("clear and iter", `Quick, iter_after_clear);
      ("clear and find", `Quick, find_after_clear);
      ("open after clear", `Quick, open_after_clear);
    ]
end

(* Tests of behaviour after restarting the index *)
module DuplicateInstance = struct
  let find_present () =
    let Context.{ rw; tbl; clone } = Context.full_index () in
    let rw2 = clone ~readonly:false () in
    check_equivalence rw tbl;
    Index.close rw;
    Index.close rw2

  let find_absent () =
    let Context.{ rw; tbl; clone } = Context.full_index () in
    let rw2 = clone ~readonly:false () in
    test_find_absent rw tbl;
    Index.close rw;
    Index.close rw2

  let replace () =
    let Context.{ rw; clone; _ } = Context.full_index ~size:5 () in
    let rw2 = clone ~readonly:false () in
    test_replace rw;
    Index.close rw;
    Index.close rw2

  let membership () =
    let Context.{ tbl; clone; rw } = Context.full_index () in
    let rw2 = clone ~readonly:false () in
    check_equivalence_mem rw2 tbl;
    Index.close rw;
    Index.close rw2

  let fail_restart_fresh () =
    let reuse_name = Context.fresh_name "empty_index" in
    let rw = Index.v ~fresh:true ~readonly:false ~log_size:4 reuse_name in
    let exn = I.RO_not_allowed in
    Alcotest.check_raises "Index readonly cannot be fresh." exn (fun () ->
        ignore_index (Index.v ~fresh:true ~readonly:true ~log_size:4 reuse_name));
    Index.close rw

  let sync () =
    let Context.{ rw; clone; _ } = Context.full_index () in
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    let rw2 = clone ~readonly:false () in
    let k2, v2 = (Key.v (), Value.v ()) in
    Index.replace rw2 k2 v2;
    check_index_entry rw k2 v2;
    check_index_entry rw2 k1 v1;
    Index.close rw;
    Index.close rw2

  let tests =
    [
      ("find (present)", `Quick, find_present);
      ("find (absent)", `Quick, find_absent);
      ("replace", `Quick, replace);
      ("membership", `Quick, membership);
      ("fail restart readonly fresh", `Quick, fail_restart_fresh);
      ("in sync", `Quick, sync);
    ]
end

(* Tests of read-only indices *)
module Readonly = struct
  let readonly () =
    let Context.{ rw; clone; _ } = Context.empty_index () in
    let ro = clone ~readonly:true () in
    Hashtbl.iter (fun k v -> Index.replace rw k v) main.tbl;
    Index.flush rw;
    Index.sync ro;
    check_equivalence ro main.tbl;
    Index.close rw;
    Index.close ro

  let readonly_v_after_replace () =
    let Context.{ rw; clone; _ } = Context.full_index () in
    let k = Key.v () in
    let v = Value.v () in
    Index.replace rw k v;
    let ro = clone ~readonly:true () in
    Index.close rw;
    Index.close ro;
    let rw = clone ~readonly:false () in
    check_index_entry rw k v;
    Index.close rw

  let readonly_clear () =
    let Context.{ rw; tbl; clone } = Context.full_index () in
    let ro = clone ~readonly:true () in
    check_equivalence ro tbl;
    Index.clear rw;
    Index.sync ro;
    Hashtbl.iter
      (fun k _ ->
        Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k)
          Not_found (fun () -> ignore_value (Index.find ro k)))
      tbl;
    Index.close rw;
    Index.close ro

  let fail_readonly_add () =
    let Context.{ rw; clone; _ } = Context.empty_index () in
    let ro = clone ~readonly:true () in
    let exn = I.RO_not_allowed in
    Alcotest.check_raises "Index readonly cannot write." exn (fun () ->
        Index.replace ro (Key.v ()) (Value.v ()));
    Index.close rw;
    Index.close ro

  (** Tests that the entries that are not flushed cannot be read by a readonly
      index. The test relies on the fact that, for log_size > 0, adding one
      entry into an empty index does not lead to flush/merge. *)
  let fail_readonly_read () =
    let Context.{ rw; clone; _ } = Context.empty_index () in
    let ro = clone ~readonly:true () in
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.sync ro;
    Alcotest.check_raises "Index readonly cannot read if data is not flushed."
      Not_found (fun () -> ignore_value (Index.find ro k1));
    Index.close rw;
    Index.close ro

  let readonly_v_in_sync () =
    let Context.{ rw; clone; _ } = Context.full_index () in
    let k = Key.v () in
    let v = Value.v () in
    Index.replace rw k v;
    Index.flush rw;
    let ro = clone ~readonly:true () in
    check_index_entry ro k v;
    Index.close rw;
    Index.close ro

  (** Readonly finds value in log before and after clear. Before sync the
      deleted value is still found. *)
  let readonly_add_log_before_clear () =
    let Context.{ rw; clone; _ } = Context.empty_index () in
    let ro = clone ~readonly:true () in
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.flush rw;
    Index.sync ro;
    check_index_entry ro k1 v1;
    Index.clear rw;
    check_index_entry ro k1 v1;
    Index.sync ro;
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find ro k1));
    Index.close rw;
    Index.close ro

  (** Readonly finds value in index before and after clear. Before sync the
      deleted value is still found. *)
  let readonly_add_index_before_clear () =
    let Context.{ rw; clone; _ } = Context.full_index () in
    let ro = clone ~readonly:true () in
    Index.clear rw;
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    let thread = Index.force_merge rw in
    Index.await thread |> check_completed;
    Index.sync ro;
    check_index_entry ro k1 v1;
    Index.clear rw;
    check_index_entry ro k1 v1;
    Index.sync ro;
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find ro k1));
    Index.close rw;
    Index.close ro

  (** Readonly finds old value in log after clear and after new values are
      added, before a sync. *)
  let readonly_add_after_clear () =
    let Context.{ rw; clone; _ } = Context.empty_index () in
    let ro = clone ~readonly:true () in
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.flush rw;
    Index.sync ro;
    check_index_entry ro k1 v1;
    Index.clear rw;
    let k2, v2 = (Key.v (), Value.v ()) in
    Index.replace rw k2 v2;
    Index.flush rw;
    check_index_entry ro k1 v1;
    Index.sync ro;
    check_index_entry ro k2 v2;
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find rw k1));
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find ro k1));
    Index.close rw;
    Index.close ro

  (** Readonly finds old value in index after clear and after new values are
      added, before a sync. This is because the readonly instance still uses the
      old index file, before being replaced by the merge. *)
  let readonly_add_index_after_clear () =
    let Context.{ rw; clone; _ } = Context.empty_index () in
    let ro = clone ~readonly:true () in
    Index.clear rw;
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    let t = Index.force_merge rw in
    Index.await t |> check_completed;
    Index.sync ro;
    Index.clear rw;
    let k2, v2 = (Key.v (), Value.v ()) in
    Index.replace rw k2 v2;
    let t = Index.force_merge rw in
    Index.await t |> check_completed;
    check_index_entry ro k1 v1;
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find rw k1));
    Index.sync ro;
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find ro k1));
    check_index_entry ro k2 v2;
    Index.close rw;
    Index.close ro

  let readonly_open_after_clear () =
    let Context.{ clone; rw; _ } = Context.full_index () in
    Index.clear rw;
    let ro = clone ~fresh:false ~readonly:true () in
    Alcotest.check_raises "Finding absent should raise Not_found" Not_found
      (fun () -> Key.v () |> Index.find ro |> ignore_value);
    Index.close ro;
    Index.close rw

  let tests =
    [
      ("add", `Quick, readonly);
      ("read after clear", `Quick, readonly_clear);
      ("Readonly v after replace", `Quick, readonly_v_after_replace);
      ("add not allowed", `Quick, fail_readonly_add);
      ("fail read if no flush", `Quick, fail_readonly_read);
      ("readonly v is in sync", `Quick, readonly_v_in_sync);
      ( "read values added in log before clear",
        `Quick,
        readonly_add_log_before_clear );
      ( "read values added in index before clear",
        `Quick,
        readonly_add_index_before_clear );
      ("read old values in log after clear", `Quick, readonly_add_after_clear);
      ( "read old values in index after clear",
        `Quick,
        readonly_add_index_after_clear );
      ("readonly open after clear", `Quick, readonly_open_after_clear);
    ]
end

(* Tests of {Index.close} *)
module Close = struct
  let close_reopen_rw () =
    let Context.{ rw; tbl; clone } = Context.full_index () in
    Index.close rw;
    let w = clone ~readonly:false () in
    check_equivalence w tbl;
    Index.close w

  let find_absent () =
    let Context.{ rw; tbl; clone; _ } = Context.full_index () in
    Index.close rw;
    let rw = clone ~readonly:false () in
    test_find_absent rw tbl;
    Index.close rw

  let replace () =
    let Context.{ rw; clone; _ } = Context.full_index ~size:5 () in
    Index.close rw;
    let rw = clone ~readonly:false () in
    test_replace rw;
    Index.close rw

  let open_readonly_close_rw () =
    let Context.{ rw; tbl; clone } = Context.full_index () in
    let ro = clone ~readonly:true () in
    Index.close rw;
    check_equivalence ro tbl;
    Index.close ro

  let close_reopen_readonly () =
    let Context.{ rw; tbl; clone } = Context.full_index () in
    Index.close rw;
    let ro = clone ~readonly:true () in
    check_equivalence ro tbl;
    Index.close ro

  let fail_api_after_close () =
    let k = Key.v () in
    let v = Value.v () in
    let calls t =
      [
        ("clear", fun () -> Index.clear t);
        ("find", fun () -> ignore_value (Index.find t k : string));
        ("mem", fun () -> ignore_bool (Index.mem t k : bool));
        ("replace", fun () -> Index.replace t k v);
        ("iter", fun () -> Index.iter (fun _ _ -> ()) t);
        ( "force_merge",
          fun () ->
            let thread = Index.force_merge t in
            Index.await thread |> function
            | Ok `Completed -> ()
            | Ok `Aborted | Error _ ->
                Alcotest.fail
                  "Unexpected return status from [force_merge] after close" );
        ("flush", fun () -> Index.flush t);
      ]
    in
    let check_calls ~readonly instance =
      Index.close instance;
      List.iter
        (fun (name, call) ->
          Alcotest.check_raises
            (Printf.sprintf "%s after close with readonly=%b raises Closed" name
               readonly)
            I.Closed call)
        (calls instance)
    in
    check_calls ~readonly:true (Context.full_index ()).rw;
    check_calls ~readonly:false (Context.full_index ()).rw

  let check_double_close () =
    let Context.{ rw; _ } = Context.full_index () in
    Index.close rw;
    Index.close rw;
    Alcotest.check_raises "flush after double close with raises Closed" I.Closed
      (fun () -> Index.flush rw)

  let restart_twice () =
    let Context.{ rw; clone; _ } = Context.empty_index () in
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.close rw;
    let rw = clone ~fresh:true ~readonly:false () in
    Alcotest.check_raises "Index restarts fresh cannot read data." Not_found
      (fun () -> ignore_value (Index.find rw k1));
    Index.close rw;
    let rw = clone ~fresh:false ~readonly:false () in
    Alcotest.check_raises "Index restarted fresh once cannot read data."
      Not_found (fun () -> ignore_value (Index.find rw k1));
    Index.close rw

  (** [close] terminates an ongoing merge operation *)
  let aborted_merge () =
    let Context.{ rw; _ } = Context.full_index ~size:100 () in
    let close_request, abort_signalled =
      (* Both locks are initially held.
         - [close_request] is dropped by the merge thread in the [`Before] hook
           as a signal to the parent thread to issue the [close] operation.

         - [abort_signalled] is dropped by the parent thread to signal to the
           child to continue the merge operation (which must then abort prematurely).
      *)
      let m1, m2 = (Mutex.create (), Mutex.create ()) in
      Mutex.lock m1;
      Mutex.lock m2;
      (m1, m2)
    in
    let hook = function
      | `Before ->
          Fmt.pr "Child: issuing request to close the index\n%!";
          Mutex.unlock close_request
      | `After_clear | `After ->
          Alcotest.fail "Merge completed despite concurrent close"
    in
    let merge_promise : _ Index.async =
      Index.force_merge ~hook:(I.Private.Hook.v hook) rw
    in
    Fmt.pr "Parent: waiting for request to close the index\n%!";
    Mutex.lock close_request;
    Fmt.pr "Parent: closing the index\n%!";
    Index.close'
      ~hook:
        (I.Private.Hook.v (fun `Abort_signalled -> Mutex.unlock abort_signalled))
      rw;
    Fmt.pr "Parent: awaiting merge result\n%!";
    Index.await merge_promise |> function
    | Ok `Completed ->
        Alcotest.fail "Force_merge returned `Completed despite concurrent close"
    | Error (`Async_exn exn) ->
        Alcotest.failf "Asynchronous exception occurred during force_merge: %s"
          (Printexc.to_string exn)
    | Ok `Aborted -> ()

  let tests =
    [
      ("close and reopen", `Quick, close_reopen_rw);
      ("find (absent)", `Quick, find_absent);
      ("replace", `Quick, replace);
      ("open two instances, close one", `Quick, open_readonly_close_rw);
      ("close and reopen on readonly", `Quick, close_reopen_readonly);
      ("non-close operations fail after close", `Quick, fail_api_after_close);
      ("double close", `Quick, check_double_close);
      ("double restart", `Quick, restart_twice);
      ("aborted merge", `Quick, aborted_merge);
    ]
end

(* Tests of {Index.filter} *)
module Filter = struct
  (** Test that all bindings are kept when using [filter] with a true predicate. *)
  let filter_none () =
    let Context.{ rw; tbl; _ } = Context.full_index () in
    Index.filter rw (fun _ -> true);
    check_equivalence rw tbl;
    Index.close rw

  (** Test that all bindings are removed when using [filter] with a false
      predicate. *)
  let filter_all () =
    let Context.{ rw; _ } = Context.full_index () in
    Index.filter rw (fun _ -> false);
    check_equivalence rw (Hashtbl.create 0);
    Index.close rw

  (** Test that [filter] can be used to remove exactly a single binding. *)
  let filter_one () =
    let Context.{ rw; tbl; _ } = Context.full_index () in
    let k = random_existing_key tbl in
    Hashtbl.remove tbl k;
    Index.filter rw (fun (k', _) -> not (String.equal k k'));
    check_equivalence rw tbl;
    Index.close rw

  (** Test that the results of [filter] are propagated to a clone which was
      created before. *)
  let clone_then_filter () =
    let Context.{ rw; tbl; clone } = Context.full_index () in
    let k = random_existing_key tbl in
    Hashtbl.remove tbl k;
    let rw2 = clone ~readonly:false () in
    Index.filter rw (fun (k', _) -> not (String.equal k k'));
    check_equivalence rw tbl;
    check_equivalence rw2 tbl;
    Index.close rw;
    Index.close rw2

  (** Test that the results of [filter] are propagated to a clone which was
      created after. *)
  let filter_then_clone () =
    let Context.{ rw; tbl; clone } = Context.full_index () in
    let k = random_existing_key tbl in
    Hashtbl.remove tbl k;
    Index.filter rw (fun (k', _) -> not (String.equal k k'));
    let rw2 = clone ~readonly:false () in
    check_equivalence rw tbl;
    check_equivalence rw2 tbl;
    Index.close rw;
    Index.close rw2

  (** Test that using [filter] doesn't affect fresh clones created later at the
      same path. *)
  let empty_after_filter_and_fresh () =
    let Context.{ rw; tbl; clone } = Context.full_index () in
    let k = random_existing_key tbl in
    Hashtbl.remove tbl k;
    Index.filter rw (fun (k', _) -> not (String.equal k k'));
    let rw2 = clone ~fresh:true ~readonly:false () in
    (* rw2 should be empty since it is fresh. *)
    check_equivalence rw2 (Hashtbl.create 0);
    Index.close rw;
    Index.close rw2

  let tests =
    [
      ("filter none", `Quick, filter_none);
      ("filter all", `Quick, filter_all);
      ("filter one", `Quick, filter_one);
      ("clone then filter", `Quick, clone_then_filter);
      ("filter then clone", `Quick, filter_then_clone);
      ("empty after filter+fresh", `Quick, empty_after_filter_and_fresh);
    ]
end

let () =
  Common.report ();
  Alcotest.run "index.unix"
    [
      ("io_array", Io_array.tests);
      ("merge", Force_merge.tests);
      ("live", Live.tests);
      ("on restart", DuplicateInstance.tests);
      ("readonly", Readonly.tests);
      ("close", Close.tests);
      ("filter", Filter.tests);
    ]
