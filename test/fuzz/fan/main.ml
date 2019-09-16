open Crowbar
module Fan = Index.Private.Fan

let hash_size = 30

let entry_size = 56

let entry_sizeL = Int64.of_int entry_size

let int_bound = 100_000_000

let bounded_int = map [ int ] (fun i -> abs i mod int_bound)

let hash = map [ bytes ] Hashtbl.hash

let hash_list = map [ list hash ] (List.sort compare)

let empty_fan_with_size =
  map [ bounded_int ] (fun n -> (Fan.v ~hash_size ~entry_size n, n))

let empty_fan = map [ empty_fan_with_size ] fst

let update_list =
  let rec loop off acc = function
    | [] -> List.rev acc
    | hash :: t -> loop (Int64.add off entry_sizeL) ((hash, off) :: acc) t
  in
  map [ hash_list ] (loop 0L [])

let fan_with_updates =
  map [ empty_fan; update_list ] (fun fan l ->
      List.iter (fun (hash, off) -> Fan.update fan hash off) l;
      Fan.finalize fan;
      (fan, l))

let fan = map [ fan_with_updates ] fst

let check_export_size fan =
  let expected_size = Fan.exported_size fan in
  let exported = Fan.export fan in
  check_eq ~pp:pp_int (String.length exported) expected_size

let check_export fan =
  let exported = Fan.export fan in
  let imported = Fan.import ~hash_size exported in
  check_eq ~eq:Fan.equal imported fan

let check_updates (fan, updates) =
  List.iter
    (fun (hash, off) ->
      let low, high = Fan.search fan hash in
      if off < low || high < off then
        (* Use Crowbar.failf on next release *)
        fail
          (Printf.sprintf
             "hash %d was added at off %Ld, but got low=%Ld, high=%Ld" hash off
             low high))
    updates

let check_fan_size (fan, size) =
  let nb_fans = Fan.nb_fans fan in
  let fan_size = size / nb_fans in
  if fan_size * entry_size > 4096 then
    fail (Printf.sprintf "Fan size is too big: %d" fan_size)

let () =
  add_test ~name:"Export size" [ fan ] check_export_size;
  add_test ~name:"Export/Import" [ fan ] check_export;
  add_test ~name:"Update" [ fan_with_updates ] check_updates;
  add_test ~name:"Fan size" [ empty_fan_with_size ] check_fan_size
