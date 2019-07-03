let string_size = 20

let index_size = 100

module Key = struct
  (* XXX Generate only 10 long strings here. *)
  type t = string

  let hash = Hashtbl.hash

  let encode s = s

  let decode s off = String.sub s off string_size

  let encoded_size = string_size

  let equal = String.equal
end

module Value = struct
  (* XXX Generate only 10 long strings here. *)
  type t = string

  let encode s = s

  let decode s off = String.sub s off string_size

  let encoded_size = string_size
end

let () = Random.self_init ()

let random_char () = char_of_int (Random.int 256)

let random_string () = String.init string_size (fun _i -> random_char ())

module Index = Deudex_unix.Make (Key) (Value)

let index_name = "hello"

let log_size = 5

let page_size = 2

let pool_size = 2

let fan_out_size = 16

let t =
  Index.v ~fresh:true ~log_size ~page_size ~pool_size ~fan_out_size index_name

let l = List.init index_size (fun _ -> (random_string (), random_string ()))

let () = List.iter (fun (k, v) -> Index.append t k v) l

let rec random_new_string () =
  let r = random_string () in
  try
    let _ = List.assoc r l in
    random_new_string ()
  with Not_found -> r

let test_find_present t =
  List.iter
    (fun (k, v) ->
      match Index.find t k with
      | None ->
          Alcotest.fail
            (Printf.sprintf "Inserted value is not present anymore: %s." k)
      | Some s -> Alcotest.(check bool) "" true (String.equal s v) )
    (List.rev l)

let test_find_absent t =
  let rec loop i =
    if i = 0 then ()
    else
      let k = random_new_string () in
      ( match Index.find t k with
      | None -> ()
      | Some _ ->
          Alcotest.fail (Printf.sprintf "Absent value was found: %s." k) );
      loop (i - 1)
  in
  loop index_size

let find_present_live () = test_find_present t

let find_absent_live () = test_find_absent t

let find_present_restart () =
  test_find_present
    (Index.v ~fresh:false ~log_size ~page_size ~pool_size ~fan_out_size
       index_name)

let find_absent_restart () =
  test_find_absent
    (Index.v ~fresh:false ~log_size ~page_size ~pool_size ~fan_out_size
       index_name)

let length_live () = Alcotest.(check int) "" index_size (Index.length t)

let length_restart () =
  let t =
    Index.v ~fresh:false ~log_size ~page_size ~pool_size ~fan_out_size
      index_name
  in
  Alcotest.(check int) "" index_size (Index.length t)

let live_tests =
  [ ("find (present)", `Quick, find_present_live);
    ("find (absent)", `Quick, find_absent_live);
    ("length", `Quick, length_live)
  ]

let restart_tests =
  [ ("find (present)", `Quick, find_present_restart);
    ("find (absent)", `Quick, find_absent_restart);
    ("length", `Quick, length_restart)
  ]

let () =
  Alcotest.run "deudex" [ ("live", live_tests); ("on restart", restart_tests) ]
