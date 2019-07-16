let key_size = 44

let value_size = 12

let index_size = 3_000_000

let () = Random.self_init ()

let random_char () = char_of_int (Random.int 256)

let random_string size = String.init size (fun _i -> random_char ())

module Key = struct
  type t = string

  let v () = random_string key_size

  let hash = Hashtbl.hash

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

let fan_out_size = 256

let t = Index.v ~fresh:true ~log_size ~fan_out_size index_name

let pp_stats ppf bindings = Fmt.pf ppf "%4dk bindings added" (bindings / 1000)

let () =
  let t0 = Sys.time () in
  let bindings = ref 0 in
  Fmt.epr "Adding %d bindings.\n%!" index_size;
  let rec loop i =
    if i = 0 then ()
    else (
      if !bindings mod 1_000 = 0 then Fmt.epr "\r%a%!" pp_stats !bindings;
      let k, v = (Key.v (), Value.v ()) in
      Index.replace t k v;
      incr bindings;
      loop (i - 1) )
  in
  loop index_size;
  let t = Sys.time () -. t0 in
  Fmt.epr "\n%d bindings added in %fs.\n%!" index_size t;
  print_newline ()
