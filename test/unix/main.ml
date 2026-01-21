module Hook = Index.Private.Hook
module Layout = Index.Private.Layout
module Semaphore = Semaphore_compat.Semaphore.Binary
module I = Index
module IO = Index_unix.Private.IO
module Int63 = Optint.Int63
open Common

type index = Common.Index.t

let ( // ) = Filename.concat
let root = "_tests" // "unix.main"

module Context = Common.Make_context (struct
  let root = root
end)

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

let test_replace t =
  let k = Key.v () in
  let v = Value.v () in
  let v' = Value.v () in
  Index.replace t k v;
  Index.replace t k v';
  Index.check_binding t k v'

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
    let* Context.{ rw; tbl; _ } = Context.with_full_index () in
    check_equivalence rw tbl

  let find_absent_live () =
    let* Context.{ rw; tbl; _ } = Context.with_full_index () in
    test_find_absent rw tbl

  let replace_live () =
    let* Context.{ rw; _ } = Context.with_full_index () in
    test_replace rw

  let different_size_for_key () =
    let* Context.{ rw; _ } = Context.with_empty_index () in
    let k = String.init 2 (fun _i -> random_char ()) in
    let v = Value.v () in
    let exn = I.Private.Data.Invalid_size k in
    Alcotest.check_raises
      "Cannot add a key of a different size than string_size." exn (fun () ->
        Index.replace rw k v)

  let different_size_for_value () =
    let* Context.{ rw; _ } = Context.with_empty_index () in
    let k = Key.v () in
    let v = String.init 200 (fun _i -> random_char ()) in
    let exn = I.Private.Data.Invalid_size v in
    Alcotest.check_raises
      "Cannot add a value of a different size than string_size." exn (fun () ->
        Index.replace rw k v)

  let membership () =
    let* Context.{ rw; tbl; _ } = Context.with_full_index () in
    check_equivalence_mem rw tbl

  let iter_after_clear () =
    let* Context.{ rw; _ } = Context.with_full_index () in
    let () = Index.clear rw in
    Index.iter (fun _ _ -> Alcotest.fail "Indexed not cleared.") rw

  let find_after_clear () =
    let* Context.{ rw; tbl; _ } = Context.with_full_index () in
    let () = Index.clear rw in
    Hashtbl.fold
      (fun k _ () ->
        match Index.find rw k with
        | exception Not_found -> ()
        | _ -> Alcotest.fail "Indexed not cleared.")
      tbl ()

  let open_after_clear () =
    let* Context.{ clone; rw; _ } = Context.with_full_index () in
    Index.clear rw;
    let rw2 = clone ~fresh:false ~readonly:false () in
    Alcotest.check_raises "Finding absent should raise Not_found" Not_found
      (fun () -> Key.v () |> Index.find rw2 |> ignore_value)

  let files_on_disk_after_clear () =
    let root = Context.fresh_name "full_index" in
    let rw = Index.v ~fresh:true ~log_size:Default.log_size root in
    for _ = 1 to Default.size do
      let k = Key.v () in
      let v = Value.v () in
      Index.replace rw k v
    done;
    Index.flush rw;
    Index.clear rw;
    Index.close rw;
    let test_there path =
      match IO.v_readonly path with
      | Error `No_file_on_disk -> Alcotest.failf "expected file: %s" path
      | Ok data ->
          Alcotest.(check int) path (IO.size data) (IO.size_header data);
          IO.close data
    in
    let test_not_there path =
      match IO.v_readonly path with
      | Error `No_file_on_disk -> ()
      | Ok _ -> Alcotest.failf "do not expect file: %s" path
    in
    test_there (Layout.log ~root);
    test_not_there (Layout.log_async ~root);
    test_not_there (Layout.data ~root)

  let duplicate_entries () =
    let* Context.{ rw; _ } = Context.with_empty_index () in
    let k1, v1, v2, v3 = (Key.v (), Value.v (), Value.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.replace rw k1 v2;
    Index.replace rw k1 v3;
    let thread = Index.try_merge_aux ~force:true rw in
    Index.await thread |> check_completed;
    let once = ref true in
    Index.iter
      (fun k v ->
        if !once && k = k1 && v = v3 then once := false
        else Alcotest.fail "Index should contain a single entry.")
      rw

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
      ("files on disk after clear", `Quick, files_on_disk_after_clear);
      ("duplicate entries", `Quick, duplicate_entries);
    ]
end

(* Tests of behaviour after restarting the index *)
module DuplicateInstance = struct
  let find_present () =
    let* Context.{ rw; tbl; clone; _ } = Context.with_full_index () in
    let (_ : index) = clone ~readonly:false () in
    check_equivalence rw tbl

  let find_absent () =
    let* Context.{ rw; tbl; clone; _ } = Context.with_full_index () in
    let (_ : index) = clone ~readonly:false () in
    test_find_absent rw tbl

  let replace () =
    let* Context.{ rw; clone; _ } = Context.with_full_index ~size:5 () in
    let (_ : index) = clone ~readonly:false () in
    test_replace rw

  let membership () =
    let* Context.{ tbl; clone; _ } = Context.with_full_index () in
    let rw' = clone ~readonly:false () in
    check_equivalence_mem rw' tbl

  let fail_restart_fresh () =
    let reuse_name = Context.fresh_name "empty_index" in
    let cache = Index.empty_cache () in
    let rw =
      Index.v ~cache ~fresh:true ~readonly:false ~log_size:4 reuse_name
    in
    let exn = I.RO_not_allowed in
    Alcotest.check_raises "Index readonly cannot be fresh." exn (fun () ->
        ignore_index
          (Index.v ~cache ~fresh:true ~readonly:true ~log_size:4 reuse_name));
    Index.close rw

  let sync () =
    let* Context.{ rw; clone; _ } = Context.with_full_index () in
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    let rw2 = clone ~readonly:false () in
    let k2, v2 = (Key.v (), Value.v ()) in
    Index.replace rw2 k2 v2;
    Index.check_binding rw k2 v2;
    Index.check_binding rw2 k1 v1

  let duplicate_entries () =
    let* Context.{ rw; clone; _ } = Context.with_empty_index () in
    let k1, v1, v2 = (Key.v (), Value.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.replace rw k1 v2;
    Index.close rw;
    let rw2 = clone ~readonly:false () in
    let once = ref true in
    Index.iter
      (fun k v ->
        if !once && k = k1 && v = v2 then once := false
        else Alcotest.fail "Index should contain a single entry.")
      rw2

  let tests =
    [
      ("find (present)", `Quick, find_present);
      ("find (absent)", `Quick, find_absent);
      ("replace", `Quick, replace);
      ("membership", `Quick, membership);
      ("fail restart readonly fresh", `Quick, fail_restart_fresh);
      ("in sync", `Quick, sync);
      ("duplicate entries in log", `Quick, duplicate_entries);
    ]
end

(* Tests of read-only indices *)
module Readonly = struct
  let readonly () =
    let* Context.{ rw; clone; tbl; _ } = Context.with_empty_index () in
    let ro = clone ~readonly:true () in
    let tbl2 =
      let h = Hashtbl.create 0 in
      Hashtbl.iter (fun k _ -> Hashtbl.add h k (Value.v ())) tbl;
      h
    in
    Hashtbl.iter (fun k v -> Index.replace rw k v) tbl2;
    Index.flush rw;
    Index.sync ro;
    check_equivalence ro tbl2

  let readonly_v_after_replace () =
    let* Context.{ rw; clone; _ } = Context.with_full_index () in
    let k = Key.v () in
    let v = Value.v () in
    Index.replace rw k v;
    let ro = clone ~readonly:true () in
    Index.close rw;
    Index.close ro;
    let rw = clone ~readonly:false () in
    Index.check_binding rw k v

  let readonly_clear () =
    let check_no_index_entry index k =
      Alcotest.check_raises (Fmt.str "Find %s key after clearing." k) Not_found
        (fun () -> ignore_value (Index.find index k))
    in
    let* Context.{ rw; tbl; clone; _ } =
      (* Ensure that the clear also wipes the LRU *)
      let lru_size = 10 in
      Context.with_full_index ~lru_size ()
    in
    let ro = clone ~readonly:true () in
    Index.clear rw;
    Index.sync ro;
    Log.info (fun m -> m "Checking that RO observes the empty index");
    Hashtbl.iter (fun k _ -> check_no_index_entry ro k) tbl;
    Index.close rw;
    Index.close ro;
    let rw = clone ~readonly:false () in
    let ro = clone ~readonly:true () in
    let k, v = (Key.v (), Value.v ()) in
    Index.replace rw k v;
    Index.check_binding rw k v;
    check_no_index_entry ro k;
    Index.flush rw;
    Index.sync ro;
    Index.check_binding rw k v;
    Index.check_binding ro k v;
    Index.clear rw;
    check_no_index_entry rw k;
    Index.check_binding ro k v;
    Index.sync ro;
    check_no_index_entry rw k;
    check_no_index_entry ro k

  (* If sync is called right after the generation is set, and before the old
     file is removed, the readonly instance reopens the old file. It does not
     try to reopen the file until the next generation change occurs. *)
  let readonly_io_clear () =
    let* Context.{ rw; clone; _ } = Context.with_full_index () in
    let ro = clone ~readonly:true () in
    let hook =
      Hook.v @@ function `IO_clear -> Index.sync ro | `Abort_signalled -> ()
    in
    Index.clear' ~hook rw;
    let k, v = (Key.v (), Value.v ()) in
    Index.replace rw k v;
    Index.flush rw;
    Index.sync ro;
    Index.check_binding rw k v;
    Index.check_binding ro k v

  let hashtbl_pick tbl =
    match Hashtbl.fold (fun k v acc -> (k, v) :: acc) tbl [] with
    | h :: _ -> h
    | _ -> assert false

  let before l m =
    I.Private.Hook.v @@ function
    | `Before -> Semaphore.acquire l
    | `After -> Semaphore.release m
    | _ -> ()

  let after l m =
    I.Private.Hook.v @@ function
    | `After_offset_read ->
        Semaphore.release l;
        Semaphore.acquire m
    | _ -> ()

  (* check that the ro instance is "snapshot isolated", e.g. it can read old
     values, even when rw overwrites them. *)
  let readonly_snapshot () =
    let* Context.{ rw; clone; tbl; _ } =
      Context.with_full_index ~throttle:`Overcommit_memory ()
    in
    let ro = clone ~readonly:true () in
    let tbl2 =
      let h = Hashtbl.create 0 in
      Hashtbl.iter (fun k _ -> Hashtbl.add h k (Value.v ())) tbl;
      h
    in
    let merge = Semaphore.make false and sync = Semaphore.make false in
    let k, v = hashtbl_pick tbl2 in

    Index.clear rw;
    Index.replace rw k v;
    let thread = Index.try_merge_aux ~force:true ~hook:(before merge sync) rw in
    Hashtbl.iter (Index.replace rw) tbl2;
    Index.flush rw;
    check_equivalence rw tbl2;
    check_equivalence ro tbl;
    Index.sync' ~hook:(after merge sync) ro;
    check_equivalence ro tbl2;
    Semaphore.release merge;
    Index.await thread |> check_completed

  let fail_readonly_add () =
    let* Context.{ clone; _ } = Context.with_empty_index () in
    let ro = clone ~readonly:true () in
    let exn = I.RO_not_allowed in
    Alcotest.check_raises "Index readonly cannot write." exn (fun () ->
        Index.replace ro (Key.v ()) (Value.v ()))

  (** Tests that the entries that are not flushed cannot be read by a readonly
      index. The test relies on the fact that, for log_size > 0, adding one
      entry into an empty index does not lead to flush/merge. *)
  let fail_readonly_read () =
    let* Context.{ rw; clone; _ } = Context.with_empty_index () in
    let ro = clone ~readonly:true () in
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.sync ro;
    Alcotest.check_raises "Index readonly cannot read if data is not flushed."
      Not_found (fun () -> ignore_value (Index.find ro k1))

  let readonly_v_in_sync () =
    let* Context.{ rw; clone; _ } = Context.with_full_index () in
    let k, v = (Key.v (), Value.v ()) in
    Index.replace rw k v;
    Index.flush rw;
    let ro = clone ~readonly:true () in
    Log.info (fun m ->
        m "Checking that RO observes the flushed binding %a" pp_binding (k, v));
    Index.check_binding ro k v

  (** Readonly finds value in log before and after clear. Before sync the
      deleted value is still found. *)
  let readonly_add_log_before_clear () =
    let* Context.{ rw; clone; _ } = Context.with_empty_index () in
    let ro = clone ~readonly:true () in
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.flush rw;
    Index.sync ro;
    Index.check_binding ro k1 v1;
    Index.clear rw;
    Index.check_binding ro k1 v1;
    Index.sync ro;
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find ro k1))

  (** Readonly finds value in index before and after clear. Before sync the
      deleted value is still found. *)
  let readonly_add_index_before_clear () =
    let* Context.{ rw; clone; _ } = Context.with_full_index () in
    let ro = clone ~readonly:true () in
    Index.clear rw;
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    let thread = Index.try_merge_aux ~force:true rw in
    Index.await thread |> check_completed;
    Index.sync ro;
    Index.check_binding ro k1 v1;
    Index.clear rw;
    Index.check_binding ro k1 v1;
    Index.sync ro;
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find ro k1))

  (** Readonly finds old value in log after clear and after new values are
      added, before a sync. *)
  let readonly_add_after_clear () =
    let* Context.{ rw; clone; _ } = Context.with_empty_index () in
    let ro = clone ~readonly:true () in
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.flush rw;
    Index.sync ro;
    Index.check_binding ro k1 v1;
    Index.clear rw;
    let k2, v2 = (Key.v (), Value.v ()) in
    Index.replace rw k2 v2;
    Index.flush rw;
    Index.check_binding ro k1 v1;
    Index.sync ro;
    Index.check_binding ro k2 v2;
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find rw k1));
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find ro k1))

  (** Readonly finds old value in index after clear and after new values are
      added, before a sync. This is because the readonly instance still uses the
      old index file, before being replaced by the merge. *)
  let readonly_add_index_after_clear () =
    let* Context.{ rw; clone; _ } = Context.with_empty_index () in
    let ro = clone ~readonly:true () in
    Index.clear rw;
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    let t = Index.try_merge_aux ~force:true rw in
    Index.await t |> check_completed;
    Index.sync ro;
    Index.clear rw;
    let k2, v2 = (Key.v (), Value.v ()) in
    Index.replace rw k2 v2;
    let t = Index.try_merge_aux ~force:true rw in
    Index.await t |> check_completed;
    Index.check_binding ro k1 v1;
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find rw k1));
    Index.sync ro;
    Alcotest.check_raises (Printf.sprintf "Found %s key after clearing." k1)
      Not_found (fun () -> ignore_value (Index.find ro k1));
    Index.check_binding ro k2 v2

  let readonly_open_after_clear () =
    let* Context.{ clone; rw; _ } = Context.with_full_index () in
    Index.clear rw;
    let ro = clone ~fresh:false ~readonly:true () in
    Alcotest.check_raises "Finding absent should raise Not_found" Not_found
      (fun () -> Key.v () |> Index.find ro |> ignore_value)

  let readonly_sync_and_merge () =
    let* Context.{ clone; rw; _ } = Context.with_empty_index () in
    let ro = clone ~readonly:true () in
    let replace = Semaphore.make false
    and merge = Semaphore.make false
    and sync = Semaphore.make false in
    let merge_hook =
      I.Private.Hook.v @@ function
      | `Before ->
          Semaphore.release replace;
          Semaphore.acquire merge
      | `After -> Semaphore.release sync
      | _ -> ()
    in
    let sync_hook =
      I.Private.Hook.v @@ function
      | `After_offset_read ->
          Semaphore.release merge;
          Semaphore.acquire sync
      | _ -> ()
    in
    let gen i = (String.make Key.encoded_size i, Value.v ()) in
    let k1, v1 = gen '1' in
    let k2, v2 = gen '2' in
    let k3, v3 = gen '3' in

    Index.replace rw k1 v1;
    let thread = Index.try_merge_aux ~force:true ~hook:merge_hook rw in
    Semaphore.acquire replace;
    Index.replace rw k2 v2;
    Index.replace rw k3 v3;
    Semaphore.release replace;
    Index.flush rw;
    Index.sync' ~hook:sync_hook ro;
    Index.await thread |> check_completed;
    Semaphore.release sync;
    Index.check_binding ro k2 v2;
    Index.check_binding ro k3 v3

  let readonly_sync_and_merge_clear () =
    let* Context.{ clone; rw; _ } = Context.with_empty_index () in
    let ro = clone ~readonly:true () in
    let merge = Semaphore.make false and sync = Semaphore.make false in
    let merge_hook =
      I.Private.Hook.v @@ function
      | `Before ->
          Semaphore.release sync;
          Semaphore.acquire merge
      | `After_clear -> Semaphore.release sync
      | _ -> ()
    in
    let sync_hook =
      I.Private.Hook.v @@ function
      | `After_offset_read ->
          Semaphore.release merge;
          Semaphore.acquire sync
      | _ -> ()
    in
    let gen i = (String.make Key.encoded_size i, Value.v ()) in
    let k1, v1 = gen '1' in
    let k2, v2 = gen '2' in

    Index.replace rw k1 v1;
    Index.flush rw;
    let thread = Index.try_merge_aux ~force:true ~hook:merge_hook rw in
    Index.replace rw k2 v2;
    Semaphore.acquire sync;
    Index.sync' ~hook:sync_hook ro;
    Index.await thread |> check_completed;
    Semaphore.release sync;
    Index.check_binding ro k1 v1

  let reload_log_async () =
    let* Context.{ rw; clone; _ } = Context.with_empty_index () in
    let ro = clone ~readonly:true () in
    let reload_log = ref 0 in
    let reload_log_async = ref 0 in
    let merge = Semaphore.make false in
    let sync = Semaphore.make false in
    let merge_hook =
      I.Private.Hook.v @@ function
      | `Before ->
          Semaphore.release sync;
          Semaphore.acquire merge
      | `After_clear -> Semaphore.release sync
      | _ -> ()
    in
    let sync_hook =
      I.Private.Hook.v (function
        | `Reload_log -> reload_log := succ !reload_log
        | `Reload_log_async -> reload_log_async := succ !reload_log_async
        | _ -> ())
    in
    let k1, v1 = (Key.v (), Value.v ()) in
    let k2, v2 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.flush rw;
    let t = Index.try_merge_aux ~force:true ~hook:merge_hook rw in
    Index.replace rw k2 v2;
    Index.flush rw;
    Semaphore.acquire sync;
    Index.sync' ~hook:sync_hook ro;
    Index.sync' ~hook:sync_hook ro;
    Index.sync' ~hook:sync_hook ro;
    Index.sync' ~hook:sync_hook ro;
    Semaphore.release merge;
    Index.check_binding ro k1 v1;
    Index.check_binding ro k2 v2;
    Alcotest.(check int) "reloadings of log per merge" 0 !reload_log;
    Alcotest.(check int) "reloadings of log async per merge" 1 !reload_log_async;
    Index.await t |> check_completed

  let tests =
    [
      ("add", `Quick, readonly);
      ("read after clear", `Quick, readonly_clear);
      ("snapshot isolation", `Quick, readonly_snapshot);
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
      ("race between sync and merge", `Quick, readonly_sync_and_merge);
      ("race between sync and clear", `Quick, readonly_io_clear);
      ( "race between sync and end of merge",
        `Quick,
        readonly_sync_and_merge_clear );
      ("reload log and log async", `Quick, reload_log_async);
    ]
end

(* Tests of {Index.close} *)
module Close = struct
  exception Stop

  let check_logs msg rw ~log_size ~log_async_size =
    let log = Index.log rw in
    let log_async = Index.log_async rw in
    let len = Option.map List.length in
    Alcotest.(check (option int)) (msg ^ ": log entries") log_size (len log);
    Alcotest.(check (option int))
      (msg ^ ": log_async entries")
      log_async_size (len log_async)

  let force_merge rw =
    let thread = Index.try_merge_aux ~force:true rw in
    Index.await thread |> check_completed;
    check_logs "force_merge" rw ~log_size:(Some 0) ~log_async_size:None

  let close_reopen_rw () =
    let* Context.{ rw; tbl; clone; _ } = Context.with_full_index () in
    Index.close rw;
    let w = clone ~readonly:false () in
    check_equivalence w tbl

  let close_reopen_rw_more () =
    let merge = Semaphore.make false in
    let merge_hook =
      I.Private.Hook.v @@ function
      | `After_clear ->
          Semaphore.release merge;
          raise Stop
      | _ -> ()
    in
    let* Context.{ rw; clone; tbl; _ } =
      Context.with_full_index ~log_size:2 ()
    in
    let k1, v1 = (Key.v (), Value.v ()) in
    let k2, v2 = (Key.v (), Value.v ()) in
    let k3, v3 = (Key.v (), Value.v ()) in

    (* await a cancelled merge and close *)
    let close rw t ~log_size ~log_async_size =
      Semaphore.acquire merge;
      check_logs "close" rw ~log_size ~log_async_size;
      Index.close ~immediately:() rw;
      match Index.await t with
      | Error (`Async_exn Stop) -> ()
      | _ -> Alcotest.fail "the merge thread should have been killed"
    in

    (* Add k1, start a merge and crash just after the index is
       renamed *)
    Index.replace rw k1 v1;
    check_logs __LOC__ rw ~log_size:(Some 1) ~log_async_size:None;
    let t = Index.try_merge_aux ~force:true ~hook:merge_hook rw in
    (* Check that the log entries have been merged with index *)
    close rw t ~log_size:(Some 0) ~log_async_size:(Some 0);

    (* k1 should be there (thx to the log file) *)
    let rw = clone ~readonly:false ~fresh:false () in
    Hashtbl.add tbl k1 v1;
    check_equivalence rw tbl;

    (* Add k2 in log and k3 in log_async and crash just after the
       index is renamed. Log is merged but log_async should still be
       present. *)
    Index.replace rw k2 v2;
    check_logs __LOC__ rw ~log_size:(Some 1) ~log_async_size:None;
    let t = Index.try_merge_aux ~force:true ~hook:merge_hook rw in
    Index.replace rw k3 v3;
    close rw t ~log_size:(Some 0) ~log_async_size:(Some 1);

    (* Reopen, k2 and k3 should be there. *)
    let rw = clone ~readonly:false ~fresh:false () in
    Hashtbl.add tbl k2 v2;
    Hashtbl.add tbl k3 v3;
    check_equivalence rw tbl

  let crash_and_continue () =
    let merge = Semaphore.make false in
    let merge_hook =
      I.Private.Hook.v @@ function
      | `After_clear ->
          Semaphore.release merge;
          raise Stop
      | _ -> ()
    in
    let* Context.{ rw; tbl; _ } = Context.with_full_index ~log_size:2 () in
    let k1, v1 = (Key.v (), Value.v ()) in
    let k2, v2 = (Key.v (), Value.v ()) in
    let k3, v3 = (Key.v (), Value.v ()) in

    let wait t =
      Semaphore.acquire merge;
      match Index.await t with
      | Error (`Async_exn Stop) -> ()
      | _ -> Alcotest.fail "the merge thread should have been killed"
    in

    (* empty log and log_async *)
    force_merge rw;

    (* Add k1, start a merge and crash just after the index is
       renamed *)
    Index.replace rw k1 v1;
    check_logs __LOC__ rw ~log_size:(Some 1) ~log_async_size:None;
    Index.try_merge_aux ~force:true ~hook:merge_hook rw |> wait;
    (* Check that the log entries have been merged with index *)
    check_logs __LOC__ rw ~log_size:(Some 0) ~log_async_size:(Some 0);

    (* k1 should be in data *)
    Hashtbl.add tbl k1 v1;
    check_equivalence rw tbl;

    (* Add k2 in log_async, as the file is present from the previous
       crash. *)
    Index.replace rw k2 v2;
    check_logs __LOC__ rw ~log_size:(Some 0) ~log_async_size:(Some 1);

    (* k2 should be there. *)
    Hashtbl.add tbl k2 v2;
    check_equivalence rw tbl;

    (* merge should merge pre-existing log_async entries  *)
    Index.try_merge_aux ~force:true ~hook:merge_hook rw |> wait;
    check_logs __LOC__ rw ~log_size:(Some 0) ~log_async_size:(Some 0);
    check_equivalence rw tbl;

    (* Add k3 in log_async as it already exists *)
    Index.replace rw k3 v3;
    check_logs __LOC__ rw ~log_size:(Some 0) ~log_async_size:(Some 1);

    (* k3 should be there. *)
    Hashtbl.add tbl k3 v3;
    check_equivalence rw tbl;

    (* full merge and check *)
    force_merge rw;
    check_equivalence rw tbl;
    Index.close rw

  let find_absent () =
    let* Context.{ rw; tbl; clone; _ } = Context.with_full_index () in
    Index.close rw;
    let rw = clone ~readonly:false () in
    test_find_absent rw tbl

  let replace () =
    let* Context.{ rw; clone; _ } = Context.with_full_index ~size:5 () in
    Index.close rw;
    let rw = clone ~readonly:false () in
    test_replace rw

  let open_readonly_close_rw () =
    let* Context.{ rw; tbl; clone; _ } = Context.with_full_index () in
    let ro = clone ~readonly:true () in
    Index.close rw;
    check_equivalence ro tbl

  let close_reopen_readonly () =
    let* Context.{ rw; tbl; clone; _ } = Context.with_full_index () in
    Index.close rw;
    let ro = clone ~readonly:true () in
    check_equivalence ro tbl

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
        ( "try_merge ~force:true",
          fun () ->
            let thread = Index.try_merge_aux ~force:true t in
            Index.await thread |> function
            | Ok `Completed -> ()
            | Ok `Aborted | Error _ ->
                Alcotest.fail
                  "Unexpected return status from [try_merge ~force:true] after \
                   close" );
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
    let* Context.{ rw; _ } = Context.with_full_index () in
    check_calls ~readonly:true rw;
    check_calls ~readonly:false rw

  let check_double_close () =
    let* Context.{ rw; _ } = Context.with_full_index () in
    Index.close rw;
    Index.close rw;
    Alcotest.check_raises "flush after double close with raises Closed" I.Closed
      (fun () -> Index.flush rw)

  let restart_twice () =
    let* Context.{ rw; clone; _ } = Context.with_empty_index () in
    let k1, v1 = (Key.v (), Value.v ()) in
    Index.replace rw k1 v1;
    Index.close rw;
    let rw = clone ~fresh:true ~readonly:false () in
    Alcotest.check_raises "Index restarts fresh cannot read data." Not_found
      (fun () -> ignore_value (Index.find rw k1));
    Index.close rw;
    let rw = clone ~fresh:false ~readonly:false () in
    Alcotest.check_raises "Index restarted fresh once cannot read data."
      Not_found (fun () -> ignore_value (Index.find rw k1))

  (** [close] terminates an ongoing merge operation *)
  let aborted_merge () =
    let* Context.{ rw; _ } =
      Context.with_full_index ~throttle:`Block_writes ~size:100 ()
    in
    let close_request, abort_signalled =
      (* Both semaphores are initially held.
         - [close_request] is dropped by the merge thread in the [`Before] hook
           as a signal to the parent thread to issue the [close] operation.

         - [abort_signalled] is dropped by the parent thread to signal to the
           child to continue the merge operation (which must then abort prematurely).
      *)
      (Semaphore.make false, Semaphore.make false)
    in
    let hook = function
      | `Before ->
          Log.app (fun f ->
              f "Child (pid = %d): issuing request to close the index"
                Thread.(id (self ())));
          Semaphore.release close_request
      | `After_first_entry -> Semaphore.acquire abort_signalled
      | `After_clear | `After ->
          Alcotest.failf
            "Child (pid = %d): merge completed despite concurrent close"
            Thread.(id (self ()))
    in
    let merge_promise : _ Index.async =
      Index.try_merge_aux ~force:true ~hook:(I.Private.Hook.v hook) rw
    in
    Log.app (fun f -> f "Parent: waiting for request to close the index");
    Semaphore.acquire close_request;
    Log.app (fun f -> f "Parent: closing the index");
    Index.close'
      ~hook:
        (I.Private.Hook.v (fun `Abort_signalled ->
             Semaphore.release abort_signalled))
      ~immediately:() rw;
    Log.app (fun f -> f "Parent: awaiting merge result");
    Index.await merge_promise |> function
    | Ok `Completed ->
        Alcotest.fail
          "try_merge ~force:true returned `Completed despite concurrent close"
    | Error (`Async_exn exn) ->
        Alcotest.failf
          "Asynchronous exception occurred during try_merge ~force:true: %s"
          (Printexc.to_string exn)
    | Ok `Aborted -> (
        match Common.get_open_fd root with
        | `Ok ofd ->
            let merge, _ = Common.partition "merge" ofd in
            if List.length merge > 0 then
              Alcotest.fail "Too many file descriptors opened for merge files"
        | `Skip err -> Log.warn (fun m -> m "`aborted_fd` was skipped: %s" err))

  let tests =
    [
      ("close and reopen", `Quick, close_reopen_rw);
      ("close and reopen more", `Quick, close_reopen_rw_more);
      ("crash and continue", `Quick, crash_and_continue);
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
  (* Filtering should also affect the in-memory LRU. *)
  let lru_size = 10

  (** Test that all bindings are kept when using [filter] with a true predicate. *)
  let filter_none () =
    let* Context.{ rw; tbl; _ } = Context.with_full_index ~lru_size () in
    Index.filter rw (fun _ -> true);
    check_equivalence rw tbl

  (** Test that all bindings are removed when using [filter] with a false
      predicate. *)
  let filter_all () =
    let* Context.{ rw; _ } = Context.with_full_index ~lru_size () in
    Index.filter rw (fun _ -> false);
    check_equivalence rw (Hashtbl.create 0)

  (** Test that [filter] can be used to remove exactly a single binding. *)
  let filter_one () =
    let* Context.{ rw; tbl; _ } = Context.with_full_index ~lru_size () in
    let k = random_existing_key tbl in
    (* Ensure the key is cached in the LRU: [filter] must remove it from there too. *)
    let (_ : Value.t) = Index.find rw k in
    Hashtbl.remove tbl k;
    Index.filter rw (fun (k', _) -> not (String.equal k k'));
    Index.check_not_found rw k;
    check_equivalence rw tbl

  (** Test that the results of [filter] are propagated to a clone which was
      created before. *)
  let clone_then_filter () =
    let* Context.{ rw; tbl; clone; _ } = Context.with_full_index ~lru_size () in
    let k = random_existing_key tbl in
    Hashtbl.remove tbl k;
    let rw2 = clone ~readonly:false () in
    Index.filter rw (fun (k', _) -> not (String.equal k k'));
    check_equivalence rw tbl;
    check_equivalence rw2 tbl

  (** Test that the results of [filter] are propagated to a clone which was
      created after. *)
  let filter_then_clone () =
    let* Context.{ rw; tbl; clone; _ } = Context.with_full_index ~lru_size () in
    let k = random_existing_key tbl in
    Hashtbl.remove tbl k;
    Index.filter rw (fun (k', _) -> not (String.equal k k'));
    let rw2 = clone ~readonly:false () in
    check_equivalence rw tbl;
    check_equivalence rw2 tbl

  (** Test that using [filter] doesn't affect fresh clones created later at the
      same path. *)
  let empty_after_filter_and_fresh () =
    let* Context.{ rw; tbl; clone; _ } = Context.with_full_index ~lru_size () in
    let k = random_existing_key tbl in
    Hashtbl.remove tbl k;
    Index.filter rw (fun (k', _) -> not (String.equal k k'));
    let rw2 = clone ~fresh:true ~readonly:false () in
    (* rw2 should be empty since it is fresh. *)
    check_equivalence rw2 (Hashtbl.create 0)

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

(** Tests of [Index.v ~throttle]*)
module Throttle = struct
  let add_binding ?hook t =
    match Index.replace_random ?hook t with
    | binding, None -> binding
    | binding, Some _ ->
        Alcotest.failf "New binding %a triggered an unexpected merge operation"
          pp_binding binding

  let add_overflow_binding ?hook t =
    match Index.replace_random ?hook t with
    | binding, Some merge_result -> (binding, merge_result)
    | binding, None ->
        Alcotest.failf "Expected new binding %a to trigger a merge operation"
          pp_binding binding

  let merge () =
    let* Context.{ rw; tbl; _ } =
      Context.with_full_index ~throttle:`Overcommit_memory ()
    in
    let m = Semaphore.make false in
    let hook = Hook.v @@ function `Before -> Semaphore.acquire m | _ -> () in
    let (_ : binding) = add_binding rw in
    let merge_result = Index.try_merge_aux ~force:true ~hook rw in
    Hashtbl.iter (fun k v -> Index.replace rw k v) tbl;
    Semaphore.release m;
    Index.await merge_result |> check_completed

  let implicit_merge () =
    let log_size = 4 in
    let* Context.{ rw; _ } =
      Context.with_empty_index ~log_size ~throttle:`Overcommit_memory ()
    in
    let m = Semaphore.make false in
    let hook =
      Hook.v @@ function `Merge `Before -> Semaphore.acquire m | _ -> ()
    in
    for _ = 1 to log_size do
      ignore (add_binding rw : binding)
    done;
    Log.app (fun m -> m "Triggering an implicit merge");
    let _, merge_result = add_overflow_binding ~hook rw in
    Log.app (fun m -> m "Overcommitting to the log while a merge is ongoing");
    for _ = 1 to log_size + 1 do
      ignore (add_binding rw : binding)
    done;
    Semaphore.release m;
    Index.await merge_result |> check_completed;
    Log.app (fun m ->
        m "Triggering a second implicit merge (with an overcommitted log)");
    let _, merge_result = add_overflow_binding rw in
    Index.await merge_result |> check_completed;
    ()

  let tests =
    [
      ("force merge", `Quick, merge); ("implicit merge", `Quick, implicit_merge);
    ]
end

module Log_file = struct
  module Log_file = I.Private.Log_file.Make (IO) (Key) (Value)
  module Thread = Index_unix.Private.Thread

  let reads () =
    let root = Context.fresh_name "empty_index" in
    let log_path = Layout.log ~root in
    let log_io =
      IO.v ~fresh:false ~fan_size:Int63.zero ~generation:Int63.zero log_path
    in
    let log = Log_file.create log_io in
    let k1, v1 = (Key.v (), Value.v ()) in
    let k2, v2 = (Key.v (), Value.v ()) in
    Log_file.replace log k1 v1;
    Log_file.replace log k2 v2;
    let find = Log_file.find log in
    check_entry ~find "log" k1 v1;
    let find = Log_file.find log in
    check_entry ~find "log" k2 v2;
    let hook () = ignore_value (Log_file.find log k1) in
    let () =
      let thread =
        Thread.async (fun () ->
            let find = Log_file.find' ~hook log in
            check_entry ~find "log" k2 v2)
      in
      Thread.await thread |> function
      | Ok () -> ()
      | Error (`Async_exn exn) ->
          Alcotest.failf "Unexpected asynchronous exception: %s"
            (Printexc.to_string exn)
    in
    Log_file.close log

  let tests = [ ("interleaved reads", `Quick, reads) ]
end

let () =
  Common.report ();
  Alcotest.run "index.unix"
    [
      ("io_array", Io_array.tests);
      ("merge", Force_merge.tests);
      ("live", Live.tests);
      ("lru", Test_lru.tests);
      ("on restart", DuplicateInstance.tests);
      ("readonly", Readonly.tests);
      ("close", Close.tests);
      ("filter", Filter.tests);
      ("flush_callback", Flush_callback.tests);
      ("throttle", Throttle.tests);
      ("log file", Log_file.tests);
    ]
