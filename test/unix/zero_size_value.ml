(* Regression test for indexes whose values are encoded on zero bytes, as in
   the snapshot export of irmin-pack.

   The log decodes its entries from a reused scratch buffer. With empty values,
   this buffer holds exactly one key, and [Repr.decode_bin] returns such a
   string as is, without copying it. The decoded keys then share the scratch
   buffer, and are overwritten by the next read. *)

let root = Filename.concat "_tests" "unix.zero_size_value"
let key_size = 20

module Key = struct
  type t = string

  let t = Repr.string_of (`Fixed key_size)
  let equal = String.equal
  let hash = Hashtbl.hash
  let hash_size = 30
  let encode s = s
  let encoded_size = key_size

  (* Like [Repr.decode_bin], return [s] itself when it holds exactly one key. *)
  let decode s off =
    if off = 0 && String.length s = key_size then s
    else String.sub s off key_size
end

module Value = struct
  type t = unit

  let t = Repr.unit
  let encode () = ""
  let encoded_size = 0
  let decode _ _ = ()
end

module Index = Index_unix.Private.Make (Key) (Value) (Index.Cache.Unbounded)

let random_key () =
  String.init key_size (fun _ -> Char.chr (33 + Random.int 94))

let check_present t key =
  match Index.find t key with
  | () -> ()
  | exception Not_found -> Alcotest.failf "Key %S is missing" key

(* The LRU is disabled so that [find] reads the merged data file. *)
let with_index f =
  let t = Index.v ~fresh:true ~lru_size:0 ~log_size:1_000 root in
  Fun.protect ~finally:(fun () -> Index.close t) (fun () -> f t)

let add_random_keys t =
  let keys = List.init 500 (fun _ -> random_key ()) in
  List.iter (fun key -> Index.replace t key ()) keys;
  keys

(* The merge reads the log through [Log_file.to_sorted_seq], which decodes all
   the entries of a bucket before sorting them. *)
let merge_keeps_all_keys () =
  with_index @@ fun t ->
  let keys = add_random_keys t in
  Index.try_merge_aux ~force:true t |> Index.await |> Common.check_completed;
  List.iter (check_present t) keys

(* [iter] must give keys that survive the next read of the log. *)
let iter_gives_all_keys () =
  with_index @@ fun t ->
  let keys = add_random_keys t in
  let seen = ref [] in
  Index.iter (fun key () -> seen := key :: !seen) t;
  Alcotest.(check (slist string String.compare)) "keys given by iter" keys !seen

let tests =
  [
    ("merge keeps all keys", `Quick, merge_keeps_all_keys);
    ("iter gives all keys", `Quick, iter_gives_all_keys);
  ]
