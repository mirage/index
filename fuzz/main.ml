open Crowbar

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

(* use Crowbar.bytes_fixed at the next Crowbar release *)
let string_gen size =
  map [ bytes ] (fun s ->
      try String.sub s 0 size with Invalid_argument _ -> bad_test ())

let list1_gen =
  list1
    (map [ string_gen key_size; string_gen value_size ] (fun k v -> (k, v)))

let add l = List.iter (fun (k, v) -> Index.add t k v) l

let find_present l t =
  add l;
  List.for_all (fun (k, v) -> List.mem v (Index.find_all t k)) l

let test_mem l t =
  add l;
  List.for_all (fun (k, _) -> Index.mem t k) l

let test_add l v t =
  add l;
  let k, v' = List.hd l in
  Index.add t k v;
  let l' = Index.find_all t k in
  List.mem v l' && List.mem v' l'

let readonly l r =
  add l;
  Index.flush t;
  List.for_all (fun (k, v) -> List.mem v (Index.find_all r k)) l

let live_tests () =
  add_test ~name:"find_present" [ list1_gen ] (fun l ->
      check (find_present l t));
  add_test ~name:"test index.mem" [ list1_gen ] (fun l -> check (test_mem l t));
  add_test ~name:"add value and duplicate key"
    [ list1_gen; string_gen value_size ] (fun l v -> check (test_add l v t))

let restart_tests () =
  let w = Index.v ~fresh:false ~log_size index_name in
  add_test ~name:"on restart: find_present" [ list1_gen ] (fun l ->
      check (find_present l w));
  add_test ~name:"on restart: add value and duplicate key"
    [ list1_gen; string_gen value_size ] (fun l v -> check (test_add l v w))

(* this test fails, it need PR #37 to succeed *)
let readonly_test () =
  let r = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  add_test ~name:"readonly: find_present" [ list1_gen ] (fun l ->
      check (readonly l r))

let () =
  live_tests ();
  restart_tests ();
  readonly_test ()
