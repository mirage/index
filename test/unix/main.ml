open Index

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

let l = ref (List.init index_size (fun _ -> (Key.v (), Value.v ())))

let () =
  let () = List.iter (fun (k, v) -> Index.add t k v) !l in
  Index.flush t

let rec random_new_key () =
  let r = Key.v () in
  try
    let _ = List.assoc r !l in
    random_new_key ()
  with Not_found -> r

let test_find_present t =
  List.iter
    (fun (k, v) ->
      match Index.find_all t k with
      | [] ->
          Alcotest.fail
            (Printf.sprintf "Wrong insertion: %s key is missing." k)
      | l ->
          if not (List.mem v l) then
            Alcotest.fail
              (Printf.sprintf "Wrong insertion: %s value is missing." v))
    (List.rev !l)

let test_find_absent t =
  let rec loop i =
    if i = 0 then ()
    else
      let k = random_new_key () in
      ( match Index.find_all t k with
      | [] -> ()
      | _ -> Alcotest.fail (Printf.sprintf "Absent value was found: %s." k) );
      loop (i - 1)
  in
  loop index_size

let test_add t =
  let k, v = List.hd !l in
  let v' = Value.v () in
  Index.add t k v';
  l := (k, v') :: !l;
  match Index.find_all t k with
  | [] ->
      Alcotest.fail
        (Printf.sprintf "Inserted value is not present anymore: %s." k)
  | l ->
      if not (List.mem v l && List.mem v' l) then
        Alcotest.fail (Printf.sprintf "Adding duplicate values failed.")

let different_size_for_key () =
  let exn = Different_Sizes_Not_Allowed in
  let k = String.init 2 (fun _i -> random_char ()) in
  let v = Value.v () in
  Alcotest.check_raises
    "Cannot add a key of a different size than string_size." exn (fun () ->
      Index.add t k v)

let different_size_for_value () =
  let exn = Different_Sizes_Not_Allowed in
  let k = Key.v () in
  let v = String.init 200 (fun _i -> random_char ()) in
  Alcotest.check_raises
    "Cannot add a value of a different size than string_size." exn (fun () ->
      Index.add t k v)

let find_present_live () = test_find_present t

let find_absent_live () = test_find_absent t

let find_present_restart () =
  test_find_present (Index.v ~fresh:false ~log_size index_name)

let find_absent_restart () =
  test_find_absent (Index.v ~fresh:false ~log_size index_name)

let replace_live () = test_add t

let replace_restart () = test_add (Index.v ~fresh:false ~log_size index_name)

let readonly () =
  let w = Index.v ~fresh:true ~readonly:false ~log_size index_name in
  let r = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  List.iter (fun (k, v) -> Index.add w k v) !l;
  Index.flush w;
  List.iter
    (fun (k, v) ->
      match Index.find_all r k with
      | [] ->
          Alcotest.fail
            (Printf.sprintf "Wrong insertion: %s key is missing." k)
      | l ->
          if not (List.mem v l) then
            Alcotest.fail
              (Printf.sprintf "Wrong insertion: %s value is missing." v))
    !l

let live_tests =
  [ ("find (present)", `Quick, find_present_live);
    ("find (absent)", `Quick, find_absent_live);
    ("add", `Quick, replace_live);
    ("fail add (key)", `Quick, different_size_for_key);
    ("fail add (value)", `Quick, different_size_for_value)
  ]

let restart_tests =
  [ ("find (present)", `Quick, find_present_restart);
    ("find (absent)", `Quick, find_absent_restart);
    ("add", `Quick, replace_restart)
  ]

let readonly_tests = [ ("add", `Quick, readonly) ]

let () =
  Alcotest.run "index"
    [ ("live", live_tests);
      ("on restart", restart_tests);
      ("readonly", readonly_tests)
    ]
