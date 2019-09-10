let key_size = 44

let value_size = 12

let index_size = 3_000_000

let () = Random.self_init ()

let random_char () = char_of_int (33 + Random.int 94)

let random_string size = String.init size (fun _i -> random_char ())

module Key = struct
  type t = string

  let v () = random_string key_size

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

  let v () = random_string value_size

  let encode s = s

  let decode s off = String.sub s off value_size

  let encoded_size = value_size

  let pp s = Fmt.fmt "%s" s
end

module Index = Index_unix.Make (Key) (Value)

let index_name = "hello"

let log_size = 500_000

let pp_stats ppf (count, max) =
  Fmt.pf ppf "\t%4dk/%dk" (count / 1000) (max / 1000)

let rec replaces t bindings i =
  if i = 0 then bindings
  else
    let count = index_size - i in
    if count mod 1_000 = 0 then Fmt.epr "\r%a%!" pp_stats (count, index_size);
    let k, v = (Key.v (), Value.v ()) in
    Index.replace t k v;
    replaces t ((k, v) :: bindings) (i - 1)

let rec finds t count = function
  | [] -> ()
  | (k, _) :: tl ->
      if count mod 1_000 = 0 then Fmt.epr "\r%a%!" pp_stats (count, index_size);
      ignore (Index.find t k);
      finds t (count + 1) tl

let with_timer f =
  let t0 = Sys.time () in
  let x = f () in
  let t1 = Sys.time () -. t0 in
  (x, t1)

let () =
  Fmt.epr "Adding %d bindings.\n%!" index_size;
  let rw = Index.v ~fresh:true ~log_size index_name in
  let bindings, t1 = with_timer (fun () -> replaces rw [] index_size) in
  Fmt.epr "\n%d bindings added in %fs.\n%!" index_size t1;
  Fmt.epr "Finding %d bindings.\n%!" index_size;
  let (), t2 = with_timer (fun () -> finds rw 0 bindings) in
  Fmt.epr "\n%d bindings found in %fs (RW).\n%!" index_size t2;
  Index.close rw;
  let ro = Index.v ~readonly:true ~log_size index_name in
  let (), t3 = with_timer (fun () -> finds ro 0 bindings) in
  Index.close ro;
  Fmt.epr "\n%d bindings found in %fs (RO).\n%!" index_size t3;
  print_newline ()
