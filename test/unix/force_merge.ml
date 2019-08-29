open Common

let index_name = "test_merge"

let t = Index.v ~fresh:true ~log_size index_name

let tbl = tbl index_name

let test_find_present t =
  Hashtbl.iter
    (fun k v ->
      match Index.find t k with
      | res ->
          if not (res = v) then
            Alcotest.fail (Printf.sprintf "Replacing existing value failed.")
      | exception Not_found ->
          Alcotest.fail
            (Printf.sprintf "Inserted value is not present anymore: %s." k))
    tbl

let test_one_entry r k v =
  match Index.find r k with
  | res ->
      if not (res = v) then
        Alcotest.fail (Printf.sprintf "Replacing existing value failed.")
  | exception Not_found ->
      Alcotest.fail
        (Printf.sprintf "Inserted value is not present anymore: %s." k)

let readonly_s () =
  let w = Index.v ~fresh:true ~readonly:false ~log_size index_name in
  let r1 = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  let r2 = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  let r3 = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  Hashtbl.iter (fun k v -> Index.replace w k v) tbl;
  Index.flush w;
  test_find_present r1;
  test_find_present r2;
  test_find_present r3

let readonly () =
  let w = Index.v ~fresh:true ~readonly:false ~log_size index_name in
  let r1 = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  let r2 = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  let r3 = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  Hashtbl.iter (fun k v -> Index.replace w k v) tbl;
  Index.flush w;
  Hashtbl.iter
    (fun k v ->
      test_one_entry r1 k v;
      test_one_entry r2 k v;
      test_one_entry r3 k v)
    tbl

let readonly_and_merge () =
  let w = Index.v ~fresh:true ~readonly:false ~log_size index_name in
  let r1 = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  let r2 = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  let r3 = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  let () =
    Hashtbl.iter (fun k v -> Index.replace w k v) tbl;
    Index.flush w
  in
  let interleave () =
    let k1 = Key.v () in
    let v1 = Value.v () in
    Index.replace w k1 v1;
    Index.force_merge w k1 v1;
    test_one_entry r1 k1 v1;
    test_one_entry r2 k1 v1;
    test_one_entry r3 k1 v1;

    let k2 = Key.v () in
    let v2 = Value.v () in
    Index.replace w k2 v2;
    test_one_entry r1 k1 v1;
    Index.force_merge w k1 v1;
    test_one_entry r2 k2 v2;
    test_one_entry r3 k1 v1;

    let k2 = Key.v () in
    let v2 = Value.v () in
    let k3 = Key.v () in
    let v3 = Value.v () in
    test_one_entry r1 k1 v1;
    Index.replace w k2 v2;
    Index.force_merge w k1 v1;
    test_one_entry r1 k1 v1;
    Index.replace w k3 v3;
    Index.force_merge w k1 v1;
    test_one_entry r3 k3 v3;

    let k2 = Key.v () in
    let v2 = Value.v () in
    Index.replace w k2 v2;
    test_one_entry w k2 v2;
    Index.force_merge w k1 v1;
    test_one_entry w k2 v2;
    test_one_entry r2 k2 v2;
    test_one_entry r3 k1 v1;

    let k2 = Key.v () in
    let v2 = Value.v () in
    Index.replace w k2 v2;
    test_one_entry r2 k1 v1;
    Index.force_merge w k1 v1;
    test_one_entry w k2 v2;
    test_one_entry r2 k2 v2;
    test_one_entry r3 k2 v2
  in
  let rec loop i =
    if i = 0 then ()
    else (
      interleave ();
      loop (i - 1) )
  in
  loop 10

let readonly_tests =
  [
    ("readonly in sequence", `Quick, readonly_s);
    ("readonly interleaved", `Quick, readonly);
  ]

let merge_tests =
  [ ("readonly and merge interleaved", `Quick, readonly_and_merge) ]

let () =
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter (reporter ());
  Alcotest.run "index"
    [
      ("readonly tests", readonly_tests);
      ("merge and readonly tests", merge_tests);
    ]

(* Unix.sleep 10 *)
(* for `ps aux | grep force_merge` and `lsof -a -s -p pid` *)
