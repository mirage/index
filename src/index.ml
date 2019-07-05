module type Key = sig
  type t

  val equal : t -> t -> bool

  val hash : t -> int

  val encode : t -> string

  val decode : string -> int -> t

  val encoded_size : int
end

module type Value = sig
  type t

  val encode : t -> string

  val decode : string -> int -> t

  val encoded_size : int
end

module type IO = Io.S

module type S = sig
  type t

  type key

  type value

  val v :
    ?fresh:bool ->
    ?read_only:bool ->
    log_size:int ->
    fan_out_size:int ->
    string ->
    t

  val clear : t -> unit

  val find : t -> key -> value option

  val mem : t -> key -> bool

  val replace : t -> key -> value -> unit

  val flush : t -> unit
end

exception RO_Not_Allowed

module Make (K : Key) (V : Value) (IO : IO) = struct
  type key = K.t

  type value = V.t

  type entry = { key : key; value : value }

  let entry_size = K.encoded_size + V.encoded_size

  let entry_sizef = float_of_int entry_size

  let entry_sizeL = Int64.of_int entry_size

  let append_entry io e =
    IO.append io (K.encode e.key);
    IO.append io (V.encode e.value)

  let decode_entry bytes off =
    let string = Bytes.unsafe_to_string bytes in
    let key = K.decode string off in
    let value = V.decode string (off + K.encoded_size) in
    { key; value }

  module Tbl = Hashtbl.Make (K)

  type config = { log_size : int; fan_out_size : int; read_only : bool }

  type t = {
    config : config;
    root : string;
    index : IO.t array;
    log : IO.t;
    log_mem : entry Tbl.t;
    entries : key Bloomf.t;
  }

  let clear t =
    IO.clear t.log;
    Bloomf.clear t.entries;
    Tbl.clear t.log_mem;
    Array.iter (fun io -> IO.clear io) t.index

  let files = Hashtbl.create 0

  let ( // ) = Filename.concat

  let log_path root = root // "index.log"

  let index_path root = root // "index" // "index"

  let map_io f io =
    let max_offset = IO.offset io in
    let rec aux offset =
      if offset >= max_offset then ()
      else
        let raw = Bytes.create (entry_size * 1_000) in
        let n = IO.read io ~off:offset raw in
        let rec read_page page off =
          if off = n then ()
          else
            let entry = decode_entry page off in
            f entry;
            (read_page [@tailcall]) page (off + entry_size)
        in
        read_page raw 0;
        (aux [@tailcall]) Int64.(add offset (of_int (entry_size * 1_000)))
    in
    (aux [@tailcall]) 0L

  let v ?(fresh = false) ?(read_only = false) ~log_size ~fan_out_size root =
    let config =
      { log_size = log_size * entry_size; fan_out_size; read_only }
    in
    let log_path = log_path root in
    let index_path = index_path root in
    try
      let t = Hashtbl.find files root in
      if fresh then if read_only then raise RO_Not_Allowed else clear t;
      t
    with Not_found ->
      let entries = Bloomf.create ~error_rate:0.01 100_000_000 in
      let log_mem = Tbl.create config.log_size in
      let log = IO.v log_path in
      let index =
        Array.init config.fan_out_size (fun i ->
            let index_path = Printf.sprintf "%s.%d" index_path i in
            let index = IO.v index_path in
            if read_only then raise RO_Not_Allowed else IO.clear index;
            map_io (fun e -> Bloomf.add entries e.key) index;
            index)
      in
      if fresh then IO.clear log;
      map_io
        (fun e ->
          Tbl.add log_mem e.key e;
          Bloomf.add entries e.key)
        log;
      let t = { config; log_mem; root; log; index; entries } in
      Hashtbl.add files root t;
      t

  let get_entry t i off =
    let buf = Bytes.create entry_size in
    let _ = IO.read t.index.(i) ~off buf in
    decode_entry buf 0

  let get_entry_iff_needed t i off = function
    | Some e -> e
    | None -> get_entry t i (Int64.of_float off)

  let interpolation_search t i key =
    let hashed_key = float_of_int (K.hash key) in
    let low = 0. in
    let high = Int64.to_float (IO.offset t.index.(i)) -. entry_sizef in
    let rec search low high lowest_entry highest_entry =
      let lowest_entry = get_entry_iff_needed t i low lowest_entry in
      let highest_entry = get_entry_iff_needed t i high highest_entry in
      if high = low then
        if K.equal lowest_entry.key key then Some lowest_entry.value else None
      else
        let lowest_hash = float_of_int (K.hash lowest_entry.key) in
        let highest_hash = float_of_int (K.hash highest_entry.key) in
        if high < low || lowest_hash > hashed_key || highest_hash < hashed_key
        then None
        else
          let doff =
            floor
              ( (high -. low)
              *. (hashed_key -. lowest_hash)
              /. (highest_hash -. lowest_hash) )
          in
          let off = low +. doff -. mod_float doff entry_sizef in
          let offL = Int64.of_float off in
          let e = get_entry t i offL in
          if K.equal e.key key then Some e.value
          else if float_of_int (K.hash e.key) < hashed_key then
            (search [@tailcall]) (off +. entry_sizef) high None
              (Some highest_entry)
          else
            (search [@tailcall]) low (off -. entry_sizef) (Some lowest_entry)
              None
    in
    if high < 0. then None else (search [@tailcall]) low high None None

  let find t key =
    if not (Bloomf.mem t.entries key) then None
    else
      match Tbl.find t.log_mem key with
      | e -> Some e.value
      | exception Not_found ->
          let i = K.hash key land (t.config.fan_out_size - 1) in
          interpolation_search t i key

  let mem t key = match find t key with None -> false | Some _ -> true

  let fan_out_cache t n =
    let caches = Array.make n [] in
    Tbl.iter
      (fun k v ->
        let index = K.hash k land (n - 1) in
        caches.(index) <- (k, v) :: caches.(index))
      t.log_mem;
    Array.map
      (List.sort (fun (k, _) (k', _) -> compare (K.hash k) (K.hash k')))
      caches

  let merge_with log t i tmp =
    let offset = ref 0L in
    let get_index_entry = function
      | Some e -> Some e
      | None ->
          if !offset >= IO.offset t.index.(i) then None
          else
            let e = get_entry t i !offset in
            offset := Int64.add !offset entry_sizeL;
            Some e
    in
    let rec go last_read l =
      match get_index_entry last_read with
      | None -> List.iter (fun (_, e) -> append_entry tmp e) l
      | Some e -> (
          match l with
          | (k, v) :: r ->
              let last, rst =
                if K.equal e.key k then (
                  append_entry tmp v;
                  (None, r) )
                else
                  let hashed_e = K.hash e.key in
                  let hashed_k = K.hash k in
                  if hashed_e = hashed_k then (
                    append_entry tmp e;
                    append_entry tmp v;
                    (None, r) )
                  else if hashed_e < hashed_k then (
                    append_entry tmp e;
                    (None, l) )
                  else (
                    append_entry tmp v;
                    (Some e, r) )
              in
              if !offset >= IO.offset t.index.(i) && last = None then
                List.iter (fun (_, e) -> append_entry tmp e) rst
              else (go [@tailcall]) last rst
          | [] ->
              append_entry tmp e;
              if !offset >= IO.offset t.index.(i) then ()
              else
                let len = Int64.sub (IO.offset t.index.(i)) !offset in
                let buf = Bytes.create (min (Int64.to_int len) 4096) in
                let rec refill () =
                  let n = IO.read t.index.(i) ~off:!offset buf in
                  let buf =
                    if n = Bytes.length buf then Bytes.unsafe_to_string buf
                    else Bytes.sub_string buf 0 n
                  in
                  IO.append tmp buf;
                  (offset := Int64.(add !offset (of_int n)));
                  if !offset < IO.offset t.index.(i) then refill ()
                in
                refill () )
    in
    (go [@tailcall]) None log

  let merge t =
    let log = fan_out_cache t t.config.fan_out_size in
    let tmp_path = t.root // "tmp" // "index" in
    Array.iteri
      (fun i index ->
        let tmp_path = Format.sprintf "%s.%d" tmp_path i in
        let tmp = IO.v tmp_path in
        merge_with log.(i) t i tmp;
        IO.rename ~src:tmp ~dst:index)
      t.index;
    IO.clear t.log;
    Tbl.clear t.log_mem

  let replace t key value =
    if t.config.read_only then raise RO_Not_Allowed;
    let entry = { key; value } in
    append_entry t.log entry;
    Tbl.replace t.log_mem key entry;
    Bloomf.add t.entries key;
    if Int64.compare (IO.offset t.log) (Int64.of_int t.config.log_size) > 0
    then merge t

  let flush t = IO.flush t.log
end
