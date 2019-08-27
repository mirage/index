let string_size = 20

let index_size = 103

let () = Random.self_init ()

let random_char () = char_of_int (33 + Random.int 94)

let random_string () = String.init string_size (fun _i -> random_char ())

module Key = struct
  type t = string

  let v = random_string

  let hash = Hashtbl.hash

  let hash_size = 30

  let encode s = s

  let decode s off = String.sub s off string_size

  let encoded_size = string_size

  let equal = String.equal

  let pp s = Fmt.fmt "%s" s
end

module Value = struct
  type t = string

  let v = random_string

  let encode s = s

  let decode s off = String.sub s off string_size

  let encoded_size = string_size

  let pp s = Fmt.fmt "%s" s
end

module Index = Index_unix.Make (Key) (Value)

let index_name = "hello"

let log_size = 4

let page_size = 2

let pool_size = 2

let t = Index.v ~fresh:true ~log_size index_name

(* [tbl] is the in-memory representation of the [t], i.e. tbl should always
contain the same elements as [t] *)
let tbl =
  let tbl = Hashtbl.create 0 in
  let rec loop i =
    if i = 0 then (
      Index.flush t;
      tbl )
    else
      let k = Key.v () in
      let v = Value.v () in
      Index.replace t k v;
      Hashtbl.replace tbl k v;
      loop (i - 1)
  in
  loop index_size

let rec random_new_key () =
  let r = Key.v () in
  if Hashtbl.mem tbl r then random_new_key () else r

let test_find_present t =
  Hashtbl.iter
    (fun k v ->
      match Index.find t k with
      | exception Not_found ->
          Alcotest.fail
            (Printf.sprintf "Wrong insertion: %s key is missing." k)
      | v' ->
          if not (v = v') then
            Alcotest.fail
              (Printf.sprintf "Wrong insertion: %s value is missing." v))
    tbl

let test_find_absent t =
  let rec loop i =
    if i = 0 then ()
    else
      let k = random_new_key () in
      Alcotest.check_raises (Printf.sprintf "Absent value was found: %s." k)
        Not_found (fun () -> ignore (Index.find t k));
      loop (i - 1)
  in
  loop index_size

let test_replace t =
  let k = Key.v () in
  let v = Value.v () in
  let v' = Value.v () in
  Index.replace t k v;
  Hashtbl.replace tbl k v;
  Index.replace t k v';
  Hashtbl.replace tbl k v';
  match Index.find t k with
  | res ->
      if not (res = v') then
        Alcotest.fail (Printf.sprintf "Replacing existing value failed.")
  | exception Not_found ->
      Alcotest.fail
        (Printf.sprintf "Inserted value is not present anymore: %s." k)

let different_size_for_key () =
  let k = String.init 2 (fun _i -> random_char ()) in
  let v = Value.v () in
  let exn = Index.Invalid_Key_Size k in
  Alcotest.check_raises
    "Cannot add a key of a different size than string_size." exn (fun () ->
      Index.replace t k v)

let different_size_for_value () =
  let k = Key.v () in
  let v = String.init 200 (fun _i -> random_char ()) in
  let exn = Index.Invalid_Value_Size v in
  Alcotest.check_raises
    "Cannot add a value of a different size than string_size." exn (fun () ->
      Index.replace t k v)

let find_present_live () = test_find_present t

let find_absent_live () = test_find_absent t

let find_present_restart () =
  test_find_present (Index.v ~fresh:false ~log_size index_name)

let find_absent_restart () =
  test_find_absent (Index.v ~fresh:false ~log_size index_name)

let replace_live () = test_replace t

let replace_restart () = test_replace (Index.v ~fresh:false ~log_size index_name)

let readonly () =
  let w = Index.v ~fresh:true ~readonly:false ~log_size index_name in
  let r = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  Hashtbl.iter (fun k v -> Index.replace w k v) tbl;
  Index.flush w;
  Hashtbl.iter
    (fun k v ->
      match Index.find r k with
      | res ->
          if not (res = v) then
            Alcotest.fail
              (Printf.sprintf "Wrong insertion: %s value is missing." v)
      | exception Not_found ->
          Alcotest.fail
            (Printf.sprintf "Wrong insertion: %s key is missing." k))
    tbl

let live_tests =
  [
    ("find (present)", `Quick, find_present_live);
    ("find (absent)", `Quick, find_absent_live);
    ("replace", `Quick, replace_live);
    ("fail add (key)", `Quick, different_size_for_key);
    ("fail add (value)", `Quick, different_size_for_value);
  ]

let restart_tests =
  [
    ("find (present)", `Quick, find_present_restart);
    ("find (absent)", `Quick, find_absent_restart);
    ("replace", `Quick, replace_restart);
  ]

let readonly_tests = [ ("add", `Quick, readonly) ]

let () =
  Alcotest.run "index"
    [
      ("live", live_tests);
      ("on restart", restart_tests);
      ("readonly", readonly_tests);
    ]
