module Hook = Index.Private.Hook
open Common

let root = Filename.concat "_tests" "unix.force_merge"

module Context = Common.Make_context (struct
  let root = root
end)

let after f = Hook.v (function `After -> f () | _ -> ())

let after_clear f = Hook.v (function `After_clear -> f () | _ -> ())

let before f = Hook.v (function `Before -> f () | _ -> ())

let before_offset_read f =
  Hook.v (function `Before_offset_read -> f () | _ -> ())

let test_find_present t tbl =
  Hashtbl.iter
    (fun k v ->
      match Index.find t k with
      | res ->
          if not (res = v) then Alcotest.fail "Replacing existing value failed."
      | exception Not_found ->
          Alcotest.failf "Inserted value is not present anymore: %s." k)
    tbl

let test_one_entry r k v =
  match Index.find r k with
  | res ->
      if not (res = v) then Alcotest.fail "Replacing existing value failed."
  | exception Not_found ->
      Alcotest.failf "Inserted value is not present anymore: %s." k

let test_fd () =
  let ( >>? ) x f = match x with `Ok x -> f x | err -> err in
  (* TODO: fix these tests to take the correct directory name
           (and not break when given the wrong one) *)
  let name = "/tmp/empty" in
  (* construct an index at a known location *)
  let pid = string_of_int (Unix.getpid ()) in
  let fd_file = "tmp" in
  let lsof_command = "lsof -a -s -p " ^ pid ^ " > " ^ fd_file in
  let result =
    ( match Sys.os_type with
    | "Unix" -> `Ok ()
    | _ -> `Skip "non-UNIX operating system" )
    >>? fun () ->
    ( match Unix.system lsof_command with
    | Unix.WEXITED 0 -> `Ok ()
    | Unix.WEXITED _ ->
        `Skip "failing `lsof` command. Is `lsof` installed on your system?"
    | Unix.WSIGNALED _ | Unix.WSTOPPED _ ->
        Alcotest.fail "`lsof` command was interrupted" )
    >>? fun () ->
    let lines = ref [] in
    let extract_fd line =
      try
        let pos = Re.Str.search_forward (Re.Str.regexp name) line 0 in
        let fd = Re.Str.string_after line pos in
        lines := fd :: !lines
      with Not_found -> ()
    in
    let ic = open_in fd_file in
    let lines =
      ( try
          while true do
            extract_fd (input_line ic)
          done
        with End_of_file -> close_in ic );
      !lines
    in
    let contains sub s =
      try
        ignore (Re.Str.search_forward (Re.Str.regexp sub) s 0);
        true
      with Not_found -> false
    in
    let data, rs = List.partition (contains "data") lines in
    if List.length data > 2 then
      Alcotest.fail "Too many file descriptors opened for data files";
    let log, rs = List.partition (contains "log") rs in
    if List.length log > 2 then
      Alcotest.fail "Too many file descriptors opened for log files";
    let lock, rs = List.partition (contains "lock") rs in
    if List.length lock > 2 then
      Alcotest.fail "Too many file descriptors opened for lock files";
    if List.length rs > 0 then Alcotest.fail "Unknown file descriptors opened";
    `Ok ()
  in
  match result with
  | `Ok () -> ()
  | `Skip err -> Log.warn (fun m -> m "`test_fd` was skipped: %s" err)

let readonly_s () =
  let { Context.tbl; clone; _ } = Context.full_index () in
  let r1 = clone ~readonly:true () in
  let r2 = clone ~readonly:true () in
  let r3 = clone ~readonly:true () in
  test_find_present r1 tbl;
  test_find_present r2 tbl;
  test_find_present r3 tbl;
  test_fd ()

let readonly () =
  let { Context.tbl; clone; _ } = Context.full_index () in
  let r1 = clone ~readonly:true () in
  let r2 = clone ~readonly:true () in
  let r3 = clone ~readonly:true () in
  Hashtbl.iter
    (fun k v ->
      test_one_entry r1 k v;
      test_one_entry r2 k v;
      test_one_entry r3 k v)
    tbl;
  test_fd ()

let readonly_and_merge () =
  let { Context.rw; clone; _ } = Context.full_index () in
  let w = rw in
  let r1 = clone ~readonly:true () in
  let r2 = clone ~readonly:true () in
  let r3 = clone ~readonly:true () in
  let interleave () =
    let k1 = Key.v () in
    let v1 = Value.v () in
    Index.replace w k1 v1;
    Index.flush w;
    let t1 = Index.force_merge w in
    Index.sync r1;
    Index.sync r2;
    Index.sync r3;
    test_one_entry r1 k1 v1;
    test_one_entry r2 k1 v1;
    test_one_entry r3 k1 v1;

    let k2 = Key.v () in
    let v2 = Value.v () in
    Index.replace w k2 v2;
    Index.flush w;
    Index.sync r1;
    Index.sync r2;
    Index.sync r3;
    test_one_entry r1 k1 v1;
    let t2 = Index.force_merge w in
    test_one_entry r2 k2 v2;
    test_one_entry r3 k1 v1;

    let k2 = Key.v () in
    let v2 = Value.v () in
    let k3 = Key.v () in
    let v3 = Value.v () in
    test_one_entry r1 k1 v1;
    Index.replace w k2 v2;
    Index.flush w;
    Index.sync r1;
    let t3 = Index.force_merge w in
    test_one_entry r1 k1 v1;
    Index.replace w k3 v3;
    Index.flush w;
    Index.sync r3;
    let t4 = Index.force_merge w in
    test_one_entry r3 k3 v3;

    let k2 = Key.v () in
    let v2 = Value.v () in
    Index.replace w k2 v2;
    Index.flush w;
    Index.sync r2;
    Index.sync r3;
    test_one_entry w k2 v2;
    let t5 = Index.force_merge w in
    test_one_entry w k2 v2;
    test_one_entry r2 k2 v2;
    test_one_entry r3 k1 v1;

    let k2 = Key.v () in
    let v2 = Value.v () in
    Index.replace w k2 v2;
    Index.flush w;
    Index.sync r2;
    Index.sync r3;
    test_one_entry r2 k1 v1;
    let t6 = Index.force_merge w in
    test_one_entry w k2 v2;
    test_one_entry r2 k2 v2;
    test_one_entry r3 k2 v2;
    Index.await t1 |> check_completed;
    Index.await t2 |> check_completed;
    Index.await t3 |> check_completed;
    Index.await t4 |> check_completed;
    Index.await t5 |> check_completed;
    Index.await t6 |> check_completed
  in

  for _ = 1 to 10 do
    interleave ()
  done;
  test_fd ()

(* A force merge has an implicit flush, however, if the replace occurs at the end of the merge, the value is not flushed *)
let write_after_merge () =
  let { Context.rw; clone; _ } = Context.full_index () in
  let w = rw in
  let r1 = clone ~readonly:true () in
  let k1 = Key.v () in
  let v1 = Value.v () in
  let k2 = Key.v () in
  let v2 = Value.v () in
  Index.replace w k1 v1;
  let hook = after (fun () -> Index.replace w k2 v2) in
  let t = Index.force_merge ~hook w in
  Index.await t |> check_completed;
  Index.sync r1;
  test_one_entry r1 k1 v1;
  Alcotest.check_raises (Printf.sprintf "Absent value was found: %s." k2)
    Not_found (fun () -> ignore_value (Index.find r1 k2))

let replace_while_merge () =
  let { Context.rw; clone; _ } = Context.full_index () in
  let w = rw in
  let r1 = clone ~readonly:true () in
  let k1 = Key.v () in
  let v1 = Value.v () in
  let k2 = Key.v () in
  let v2 = Value.v () in
  Index.replace w k1 v1;
  let hook =
    before (fun () ->
        Index.replace w k2 v2;
        test_one_entry w k2 v2)
  in
  let t = Index.force_merge ~hook w in
  Index.sync r1;
  test_one_entry r1 k1 v1;
  Index.await t |> check_completed

(* note that here we cannot do
   `test_one_entry r1 k2 v2`
   as there is no way to guarantee that the latests value
   added by a RW instance is found by a RO instance
*)

let find_while_merge () =
  let { Context.rw; clone; _ } = Context.full_index () in
  let w = rw in
  let k1 = Key.v () in
  let v1 = Value.v () in
  Index.replace w k1 v1;
  let f () = test_one_entry w k1 v1 in
  let t1 = Index.force_merge ~hook:(after f) w in
  let t2 = Index.force_merge ~hook:(after f) w in
  let r1 = clone ~readonly:true () in
  let f () = test_one_entry r1 k1 v1 in
  let t3 = Index.force_merge ~hook:(before f) w in
  let t4 = Index.force_merge ~hook:(before f) w in
  Index.await t1 |> check_completed;
  Index.await t2 |> check_completed;
  Index.await t3 |> check_completed;
  Index.await t4 |> check_completed

let find_in_async_generation_change () =
  let { Context.rw; clone; _ } = Context.full_index () in
  let w = rw in
  let r1 = clone ~readonly:true () in
  let k1 = Key.v () in
  let v1 = Value.v () in
  let f () =
    Index.replace w k1 v1;
    Index.flush w;
    Index.sync r1;
    test_one_entry r1 k1 v1
  in
  let t1 = Index.force_merge ~hook:(before f) w in
  Index.await t1 |> check_completed

let find_in_async_same_generation () =
  let { Context.rw; clone; _ } = Context.full_index () in
  let w = rw in
  let r1 = clone ~readonly:true () in
  let k1 = Key.v () in
  let v1 = Value.v () in
  let k2 = Key.v () in
  let v2 = Value.v () in
  let f () =
    Index.replace w k1 v1;
    Index.flush w;
    Index.sync r1;
    test_one_entry r1 k1 v1;
    Index.replace w k2 v2;
    Index.flush w;
    Index.sync r1;
    test_one_entry r1 k2 v2
  in
  let t1 = Index.force_merge ~hook:(before f) w in
  Index.await t1 |> check_completed

(** RW adds a value in log and flushes it, so every subsequent RO sync should
    find that value. But if the RO sync occurs during a merge, after a clear but
    before a generation change, then the value is missed. Also test ro find at
    this point. *)
let sync_after_clear_log () =
  let Context.{ rw; clone; _ } = Context.empty_index () in
  let ro = clone ~readonly:true () in
  let k1, v1 = (Key.v (), Value.v ()) in
  Index.replace rw k1 v1;
  Index.flush rw;
  let hook = after_clear (fun () -> Index.sync ro) in
  let t = Index.force_merge ~hook rw in
  Index.await t |> check_completed;
  test_one_entry ro k1 v1;
  let k2, v2 = (Key.v (), Value.v ()) in
  Index.replace rw k2 v2;
  Index.flush rw;
  Index.sync ro;
  let hook = after_clear (fun () -> test_one_entry ro k1 v1) in
  let t = Index.force_merge ~hook rw in
  Index.await t |> check_completed;
  Index.close rw;
  Index.close ro

(** during a merge RO sync can miss a value if it reads the generation before
    the generation is updated. *)
let merge_during_sync () =
  let Context.{ rw; clone; _ } = Context.empty_index () in
  let ro = clone ~readonly:true () in
  let k1, v1 = (Key.v (), Value.v ()) in
  Index.replace rw k1 v1;
  Index.flush rw;
  let hook =
    before_offset_read (fun () ->
        let t = Index.force_merge rw in
        Index.await t |> check_completed)
  in
  Index.sync' ~hook ro;
  test_one_entry ro k1 v1;
  Index.close rw;
  Index.close ro

let test_is_merging () =
  let Context.{ rw; _ } = Context.empty_index () in
  let add_binding_and_merge ~hook =
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    let t = Index.force_merge ~hook rw in
    Index.await t |> check_completed
  in
  let f msg b () = Alcotest.(check bool) msg (Index.is_merging rw) b in
  f "before merge" false ();
  add_binding_and_merge ~hook:(before (f "before" true));
  f "between merge" false ();
  add_binding_and_merge ~hook:(after (f "after" true));
  add_binding_and_merge ~hook:(after_clear (f "after clear" true));
  Index.close rw

let tests =
  [
    ("readonly in sequence", `Quick, readonly_s);
    ("readonly interleaved", `Quick, readonly);
    ("interleaved merge", `Quick, readonly_and_merge);
    ("write at the end of merge", `Quick, write_after_merge);
    ("write in log_async", `Quick, replace_while_merge);
    ("find while merging", `Quick, find_while_merge);
    ("find in async without log", `Quick, find_in_async_generation_change);
    ("find in async with log", `Quick, find_in_async_same_generation);
    ("sync and find after log cleared", `Quick, sync_after_clear_log);
    ("merge during ro sync", `Quick, merge_during_sync);
    ("is_merging", `Quick, test_is_merging);
  ]
