let string_size = 20

let index_size = 103

let () = Random.self_init ()

let random_char () = char_of_int (33 + Random.int 94)

let random_string () = String.init string_size (fun _i -> random_char ())

module Key = struct
  type t = string

  let v = random_string

  let hash = Hashtbl.hash

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

let fan_out_size = 16

let t = Index.v ~fresh:true ~log_size ~fan_out_size index_name

let l = List.init index_size (fun _ -> (Key.v (), Value.v ()))

let () =
  let () = List.iter (fun (k, v) -> Index.replace t k v) l in
  Index.flush t

let rec random_new_key () =
  let r = Key.v () in
  try
    let _ = List.assoc r l in
    random_new_key ()
  with Not_found -> r

let test_find_present t =
  List.iter
    (fun (k, v) ->
      match Index.find t k with
      | None ->
          Alcotest.fail
            (Printf.sprintf "Inserted value is not present anymore: %s." k)
      | Some s -> Alcotest.(check bool) "" true (String.equal s v))
    (List.rev l)

let test_find_absent t =
  let rec loop i =
    if i = 0 then ()
    else
      let k = random_new_key () in
      ( match Index.find t k with
      | None -> ()
      | Some _ ->
          Alcotest.fail (Printf.sprintf "Absent value was found: %s." k) );
      loop (i - 1)
  in
  loop index_size

let test_replace t =
  let k, v = List.hd l in
  let v' = Value.v () in
  Index.replace t k v';
  ( match Index.find t k with
  | None ->
      Alcotest.fail
        (Printf.sprintf "Inserted value is not present anymore: %s." k)
  | Some v ->
      if v <> v' then
        Alcotest.fail
          (Printf.sprintf "Wrong replacement: got %s, expected %s." v v') );
  Index.replace t k v

let find_present_live () = test_find_present t

let find_absent_live () = test_find_absent t

let find_present_restart () =
  test_find_present (Index.v ~fresh:false ~log_size ~fan_out_size index_name)

let find_absent_restart () =
  test_find_absent (Index.v ~fresh:false ~log_size ~fan_out_size index_name)

let replace_live () = test_replace t

let replace_restart () =
  test_replace (Index.v ~fresh:false ~log_size ~fan_out_size index_name)

let readonly () =
  let w =
    Index.v ~fresh:true ~readonly:false ~log_size ~fan_out_size index_name
  in
  let r =
    Index.v ~fresh:false ~readonly:true ~log_size ~fan_out_size index_name
  in
  List.iter (fun (k, v) -> Index.replace w k v) l;
  Index.flush w;
  List.iter
    (fun (k, v') ->
      match Index.find r k with
      | None ->
          Alcotest.fail
            (Printf.sprintf "Inserted value is not present anymore: %s." k)
      | Some v ->
          if v <> v' then
            Alcotest.fail
              (Printf.sprintf "Wrong replacement: got %s, expected %s." v v'))
    l

let live_tests =
  [ ("find (present)", `Quick, find_present_live);
    ("find (absent)", `Quick, find_absent_live);
    ("replace", `Quick, replace_live)
  ]

let restart_tests =
  [ ("find (present)", `Quick, find_present_restart);
    ("find (absent)", `Quick, find_absent_restart);
    ("replace", `Quick, replace_restart)
  ]

let readonly_tests = [ ("replace", `Quick, readonly) ]

let () =
  Alcotest.run "index"
    [ ("live", live_tests);
      ("on restart", restart_tests);
      ("readonly", readonly_tests)
    ]
