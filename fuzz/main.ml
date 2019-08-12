open Crowbar
open Lwt.Infix

let index_name = "hello"

let log_size = 4

let key_size = 10

let value_size = 20

module Key = struct
  type t = string

  let hash = Hashtbl.hash

  let hash_size = 30

  let encode s = s

  let decode s off = String.sub s off key_size

  let encoded_size = key_size

  let equal = String.equal

  let pp s = Fmt.fmt "%s" s
end

module Value = struct
  type t = string

  let encode s = s

  let decode s off = String.sub s off value_size

  let encoded_size = value_size

  let pp s = Fmt.fmt "%s" s
end

module Index = Index_unix.Make (Key) (Value)

let t = Index.v ~fresh:true ~log_size index_name

let string_gen size =
  map [ bytes ] (fun s ->
      try String.sub s 0 size with Invalid_argument _ -> bad_test ())

let list_gen =
  fix (fun list_gen ->
      choose
        [ const [];
          map [ string_gen key_size; string_gen value_size; list_gen ]
            (fun k v l ->
              Index.add t k v;
              (k, v) :: l)
        ])

let find_present l t =
  let ok = List.for_all (fun (k, v) -> List.mem v (Index.find_all t k)) l in
  Index.clear t;
  ok

let no_extra_values_added l t =
  Index.iter
    (fun k v ->
      if not (List.exists (fun (k', v') -> k = k' && v = v') l) then
        fail "extra value added")
    t;
  Index.clear t;
  true

let test_mem l t =
  let ok = List.for_all (fun (k, _) -> Index.mem t k) l in
  Index.clear t;
  ok

let test_add l v t =
  let k, v' = List.hd l in
  Index.add t k v;
  let l' = Index.find_all t k in
  let ok = List.mem v l' && List.mem v' l' in
  Index.clear t;
  ok

let find_present_lwt l t =
  let ok =
    Lwt_list.for_all_p
      (fun (k, v) -> Lwt.return (List.mem v (Index.find_all t k)))
      l
  in
  Index.clear t;
  ok

let readonly l r =
  Index.flush t;
  let ok = List.for_all (fun (k, v) -> List.mem v (Index.find_all r k)) l in
  Index.clear t;
  ok

let lwt_list_gen : ((string * string) list * unit Lwt.t list) gen =
  fix (fun lwt_list_gen ->
      choose
        [ const ([], []);
          map [ string_gen key_size; string_gen value_size; lwt_list_gen ]
            (fun k v (l, lp) ->
              let p = Lwt.return (Index.add t k v) in
              ((k, v) :: l, p :: lp))
        ])

let readonly_lwt (l : (string * string) list) (wp : unit Lwt.t list) r =
  ( Lwt.join wp >|= fun _ ->
    Index.flush t )
  >>= fun _ ->
  find_present_lwt l r

let live_tests () =
  add_test ~name:"find_present" [ list_gen ] (fun l ->
      check (find_present l t));
  add_test ~name:"no extra values" [ list_gen ] (fun l ->
      check (no_extra_values_added l t));
  add_test ~name:"test index.mem" [ list_gen ] (fun l -> check (test_mem l t));
  add_test ~name:"add value and duplicate key"
    [ list_gen; string_gen value_size ] (fun l v ->
      guard (l != []);
      check (test_add l v t));
  add_test ~name:"find_present_lwt" [ list_gen ] (fun l ->
      check (Lwt_main.run (find_present_lwt l t)))

let restart_tests () =
  let w = Index.v ~fresh:false ~log_size index_name in
  add_test ~name:"on restart: find_present" [ list_gen ] (fun l ->
      check (find_present l w));
  add_test ~name:"on restart: no_extra_values" [ list_gen ] (fun l ->
      check (no_extra_values_added l w));
  add_test ~name:"on restart:add value and duplicate key"
    [ list_gen; string_gen value_size ] (fun l v ->
      guard (l != []);
      check (test_add l v w))

let readonly_test () =
  let r =
    Index.v ~shared:true ~fresh:false ~readonly:true ~log_size index_name
  in
  add_test ~name:"readonly: find_present" [ list_gen ] (fun l ->
      check (readonly l r));
  add_test ~name:"readonly: find_present_lwt" [ lwt_list_gen ] (fun (l, wp) ->
      check (Lwt_main.run (readonly_lwt l wp r)))

let () =
  live_tests ();
  restart_tests ();
  readonly_test ()
