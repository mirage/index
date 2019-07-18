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

let fan_out_size = 13

let t = Index.v ~fresh:true ~log_size ~fan_out_size index_name

let pp_stats ppf (count, max) =
  Fmt.pf ppf "\t%4dk/%dk" (count / 1000) (max / 1000)

let () =
  let t0 = Sys.time () in
  Fmt.epr "Adding %d bindings.\n%!" index_size;
  let rec loop bindings i =
    if i = 0 then bindings
    else
      let count = index_size - i in
      if count mod 1_000 = 0 then Fmt.epr "\r%a%!" pp_stats (count, index_size);
      let k, v = (Key.v (), Value.v ()) in
      Index.add t k v;
      loop ((k, v) :: bindings) (i - 1)
  in
  let bindings = loop [] index_size in
  let t1 = Sys.time () -. t0 in
  Fmt.epr "\n%d bindings added in %fs.\n%!" index_size t1;
  Fmt.epr "Finding %d bindings.\n%!" index_size;
  let rec loop count = function
    | [] -> ()
    | (k, v) :: tl -> (
        if count mod 1_000 = 0 then
          Fmt.epr "\r%a%!" pp_stats (count, index_size);
        match Index.find_all t k with
        | [] -> assert false
        | l ->
            assert (List.mem v l);
            loop (count + 1) tl )
  in
  loop 0 bindings;
  let t2 = Sys.time () -. t1 in
  Fmt.epr "\n%d bindings found in %fs.\n%!" index_size t2;
  print_newline ()
