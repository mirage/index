open Crowbar
module Fan = Index.Private.Fan

external get_64 : string -> int -> int64 = "%caml_string_get64"

external swap64 : int64 -> int64 = "%bswap_int64"

let decode_int64 buf =
  let get_uint64 s off =
    if not Sys.big_endian then swap64 (get_64 s off) else get_64 s off
  in
  get_uint64 buf 0

let hash_size = 30

let entry_size = 56

let max_contents = 100_000_000

let max_contentsL = Int64.of_int max_contents

let bounded_int = map [ int ] (fun i -> abs i mod max_contents)

let bounded_int64 =
  map [ int64 ] (fun i -> Int64.rem (Int64.abs i) max_contentsL)

let hash = map [ bytes ] Hashtbl.hash

let empty_fan = map [ bounded_int ] (Fan.v ~hash_size ~entry_size)

let update = map [ hash; bounded_int64 ] (fun hash off -> (hash, off))

let update_list = list update

let fan =
  map [ empty_fan; update_list ] (fun fan l ->
      List.iter (fun (hash, off) -> Fan.update fan hash off) l;
      fan)

let check_export_size fan =
  let expected_size = Fan.exported_size fan in
  let exported = Fan.export fan in
  check_eq ~pp:pp_int (String.length exported) expected_size

let check_export fan =
  let exported = Fan.export fan in
  let imported = Fan.import ~hash_size exported in
  check_eq ~eq:Fan.equal imported fan

let () =
  add_test ~name:"Export size" [ fan ] check_export_size;
  add_test ~name:"Export/Import" [ fan ] check_export
