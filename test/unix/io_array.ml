module IO = Index_unix.Private.IO

let ( // ) = Filename.concat

let root = "_tests" // "unix.io_array"

module Entry = struct
  module Key = Common.Key
  module Value = Common.Value

  type t = Key.t * Value.t

  let encoded_size = Key.encoded_size + Value.encoded_size

  let decode bytes off =
    let string = Bytes.unsafe_to_string bytes in
    let key = Key.decode string off in
    let value = Value.decode string (off + Key.encoded_size) in
    (key, value)

  let append_io io (key, value) =
    let encoded_key = Key.encode key in
    let encoded_value = Value.encode value in
    IO.append io encoded_key;
    IO.append io encoded_value
end

module IOArray = Index.Private.Io_array.Make (IO) (Entry)

let entry = Alcotest.(pair string string)

let fresh_io name =
  IO.v ~readonly:false ~fresh:true ~generation:0L ~fan_size:0L (root // name)

(* Append a random sequence of [size] keys to an IO instance and return
   a pair of an IOArray and an equivalent in-memory array. *)
let populate_random ~size io =
  let rec loop acc = function
    | 0 -> acc
    | n ->
        let e = (Common.Key.v (), Common.Value.v ()) in
        Entry.append_io io e;
        loop (e :: acc) (n - 1)
  in
  let mem_arr = Array.of_list (List.rev (loop [] size)) in
  let io_arr = IOArray.v io in
  IO.sync io;
  (mem_arr, io_arr)

(* Tests *)
let read_sequential () =
  let size = 1000 in
  let io = fresh_io "read_sequential" in
  let mem_arr, io_arr = populate_random ~size io in
  for i = 0 to size - 1 do
    let expected = mem_arr.(i) in
    let actual = IOArray.get io_arr (Int64.of_int i) in
    Alcotest.(check entry)
      (Fmt.strf "Inserted key at index %i is accessible" i)
      expected actual
  done

let read_sequential_prefetch () =
  let size = 1000 in
  let io = fresh_io "read_sequential_prefetch" in
  let mem_arr, io_arr = populate_random ~size io in
  IOArray.pre_fetch io_arr ~low:0L ~high:999L;

  (* Read the arrays backwards *)
  for i = size - 1 to 0 do
    let expected = mem_arr.(i) in
    let actual = IOArray.get io_arr (Int64.of_int i) in
    Alcotest.(check entry)
      (Fmt.strf "Inserted key at index %i is accessible" i)
      expected actual
  done

let tests =
  [
    ("fresh", `Quick, read_sequential);
    ("prefetch", `Quick, read_sequential_prefetch);
  ]
