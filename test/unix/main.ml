let string_size = 20

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

let l = List.init 50 (fun _ -> (random_string (), random_string ()))

let () = List.iter (fun (k, v) -> Index.append t k v) l

let test_find t =
  List.iter
    (fun (k, v) ->
      match Index.find t k with
      | None ->
          Alcotest.fail
            (Printf.sprintf "Inserted value is not present anymore: %s." k)
      | Some s -> Alcotest.(check bool) "" true (String.equal s v) )
    (List.rev l)

let test_fresh_find () = test_find t

let test_find_on_restart () =
  test_find
    (Index.v ~fresh:false ~log_size ~page_size ~pool_size ~fan_out_size
       index_name)

let () =
  Alcotest.run "deudex"
    [ ( "deudex",
        [ ("Find when element was added", `Quick, test_fresh_find);
          ("Find when element was added (on restart)", `Quick, test_fresh_find)
        ] )
    ]
