module type Key = sig
  type t

  val equal : t -> t -> bool

  val hash : t -> int

  val encode : t -> string

  val decode : string -> int -> t

  val encoded_size : int

  val pp : t Fmt.t
end

module type Value = sig
  type t

  val encode : t -> string

  val decode : string -> int -> t

  val encoded_size : int

  val pp : t Fmt.t
end

module type IO = Io.S

module type S = sig
  type t

  type key

  type value

  val v :
    ?fresh:bool ->
    ?readonly:bool ->
    ?shared:bool ->
    log_size:int ->
    fan_out_size:int ->
    string ->
    t

  val clear : t -> unit

  val find : t -> key -> value option

  val mem : t -> key -> bool

  val replace : t -> key -> value -> unit

  val iter : (key -> value -> unit) -> t -> unit

  val flush : t -> unit
end

exception RO_Not_Allowed

let src = Logs.Src.create "index" ~doc:"Index"

module Log = (val Logs.src_log src : Logs.LOG)

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

  type config = { log_size : int; fan_out_size : int; readonly : bool }

  type t = {
    config : config;
    root : string;
    mutable generation : int64;
    index : IO.t array;
    log : IO.t;
    log_mem : entry Tbl.t;
    entries : key Bloomf.t;
  }

  let clear t =
    Log.debug (fun l -> l "clear \"%s\"" t.root);
    IO.clear t.log;
    Bloomf.clear t.entries;
    Tbl.clear t.log_mem;
    Array.iter (fun io -> IO.clear io) t.index

  let ( // ) = Filename.concat

  let log_path root = root // "index.log"

  let index_path root = root // "index" // "index"

  let iter_io ?(min = 0L) ?max f io =
    let max = match max with None -> IO.offset io | Some m -> m in
    let rec aux offset =
      if offset >= max then ()
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
    (aux [@tailcall]) min

  let with_cache ~v ~clear =
    let roots = Hashtbl.create 0 in
    fun ~fresh ~shared ~readonly root ->
      if not shared then (
        Log.debug (fun l ->
            l "[%s] v fresh=%b shared=%b readonly=%b" (Filename.basename root)
              fresh shared readonly);
        v ~fresh ~readonly root )
      else
        try
          if not (Sys.file_exists root) then (
            Log.debug (fun l ->
                l "[%s] does not exist anymore, cleaning up the fd cache"
                  (Filename.basename root));
            Hashtbl.remove roots root;
            raise Not_found );
          let t = Hashtbl.find roots root in
          Log.debug (fun l -> l "%s found in cache" root);
          if fresh then clear t;
          t
        with Not_found ->
          Log.debug (fun l ->
              l "[%s] v fresh=%b shared=%b readonly=%b"
                (Filename.basename root) fresh shared readonly);
          let t = v ~fresh ~readonly root in
          Hashtbl.add roots root t;
          t

  let v_no_cache ~log_size ~fan_out_size ~fresh ~readonly root =
    let config =
      { log_size = log_size * entry_size; fan_out_size; readonly }
    in
    let log_path = log_path root in
    let index_path = index_path root in
    let entries = Bloomf.create ~error_rate:0.01 100_000_000 in
    let log_mem = Tbl.create 1024 in
    let log = IO.v ~fresh ~readonly ~generation:0L log_path in
    let index =
      Array.init config.fan_out_size (fun i ->
          let index_path = Printf.sprintf "%s.%d" index_path i in
          let index = IO.v ~fresh ~readonly ~generation:0L index_path in
          iter_io (fun e -> Bloomf.add entries e.key) index;
          index)
    in
    let generation = IO.get_generation log in
    iter_io
      (fun e ->
        Tbl.add log_mem e.key e;
        Bloomf.add entries e.key)
      log;
    { config; generation; log_mem; root; log; index; entries }

  let v ?(fresh = false) ?(readonly = false) ?(shared = true) ~log_size
      ~fan_out_size root =
    with_cache
      ~v:(v_no_cache ~log_size ~fan_out_size)
      ~clear ~fresh ~shared ~readonly root

  let get_entry t i off =
    let buf = Bytes.create entry_size in
    let _ = IO.read t.index.(i) ~off buf in
    decode_entry buf 0

  let get_entry_iff_needed t i off = function
    | Some e -> e
    | None -> get_entry t i (Int64.of_float off)

  let look_around t i key h_key off =
    let rec search op curr =
      let off = op curr entry_sizeL in
      let e = get_entry t i off in
      if K.equal e.key key then Some e.value
      else
        let h_e = K.hash e.key in
        if h_e <> h_key then None else search op off
    in
    match search Int64.add off with None -> search Int64.sub off | o -> o

  let interpolation_search t i key =
    let hashed_key = K.hash key in
    let hashed_keyf = float_of_int hashed_key in
    let low = 0. in
    let high = Int64.to_float (IO.offset t.index.(i)) -. entry_sizef in
    let rec search low high lowest_entry highest_entry =
      if high < low then None
      else
        let lowest_entry = get_entry_iff_needed t i low lowest_entry in
        if high = low then
          if K.equal lowest_entry.key key then Some lowest_entry.value
          else None
        else
          let highest_entry = get_entry_iff_needed t i high highest_entry in
          let lowest_hash = K.hash lowest_entry.key in
          let highest_hash = K.hash highest_entry.key in
          if lowest_hash > hashed_key || highest_hash < hashed_key then None
          else
            let lowest_hashf = float_of_int lowest_hash in
            let highest_hashf = float_of_int highest_hash in
            let doff =
              floor
                ( (high -. low)
                *. (hashed_keyf -. lowest_hashf)
                /. (highest_hashf -. lowest_hashf) )
            in
            let off = low +. doff -. mod_float doff entry_sizef in
            let offL = Int64.of_float off in
            let e = get_entry t i offL in
            let hashed_e = K.hash e.key in
            if hashed_key = hashed_e then
              if K.equal e.key key then Some e.value
              else look_around t i key hashed_key offL
            else if hashed_e < hashed_key then
              (search [@tailcall]) (off +. entry_sizef) high None
                (Some highest_entry)
            else
              (search [@tailcall]) low (off -. entry_sizef) (Some lowest_entry)
                None
    in
    if high < 0. then None else (search [@tailcall]) low high None None

  let sync_log t =
    let generation = IO.get_generation t.log in
    let log_offset = IO.offset t.log in
    let new_log_offset = IO.force_offset t.log in
    let add_log_entry e =
      Tbl.replace t.log_mem e.key e;
      Bloomf.add t.entries e.key
    in
    if t.generation <> generation then (
      Tbl.clear t.log_mem;
      iter_io add_log_entry t.log;
      Array.iteri
        (fun i index ->
          let path = IO.name index in
          let index = IO.v ~fresh:false ~readonly:true ~generation path in
          let _ = IO.force_offset index in
          iter_io (fun e -> Bloomf.add t.entries e.key) index;
          t.index.(i) <- index)
        t.index;
      t.generation <- generation )
    else if log_offset < new_log_offset then
      iter_io add_log_entry t.log ~min:log_offset
    else if log_offset > new_log_offset then assert false

  let find t key =
    Log.debug (fun l -> l "find \"%s\" %a" t.root K.pp key);
    if t.config.readonly then sync_log t;
    if not (Bloomf.mem t.entries key) then None
    else
      match Tbl.find t.log_mem key with
      | e -> Some e.value
      | exception Not_found ->
          let i = K.hash key land (t.config.fan_out_size - 1) in
          interpolation_search t i key

  let mem t key =
    Log.debug (fun l -> l "mem \"%s\" %a" t.root K.pp key);
    match find t key with None -> false | Some _ -> true

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
    Log.debug (fun l -> l "merge index %d" i);
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
    Log.debug (fun l -> l "merge \"%s\"" t.root);
    let log = fan_out_cache t t.config.fan_out_size in
    let tmp_path = t.root // "tmp" // "index" in
    let generation = Int64.succ t.generation in
    Array.iteri
      (fun i index ->
        let tmp_path = Format.sprintf "%s.%d" tmp_path i in
        let tmp = IO.v ~readonly:false ~fresh:true ~generation tmp_path in
        merge_with log.(i) t i tmp;
        IO.rename ~src:tmp ~dst:index)
      t.index;
    IO.clear t.log;
    Tbl.clear t.log_mem;
    IO.set_generation t.log generation;
    t.generation <- generation

  let replace t key value =
    Log.debug (fun l -> l "replace \"%s\" %a %a" t.root K.pp key V.pp value);
    if t.config.readonly then raise RO_Not_Allowed;
    let entry = { key; value } in
    append_entry t.log entry;
    Tbl.replace t.log_mem key entry;
    Bloomf.add t.entries key;
    if Int64.compare (IO.offset t.log) (Int64.of_int t.config.log_size) > 0
    then merge t

  (* XXX: Perform a merge beforehands to ensure duplicates are not hit twice. *)
  let iter f t =
    Tbl.iter (fun _ e -> f e.key e.value) t.log_mem;
    Array.iter (iter_io (fun e -> f e.key e.value)) t.index

  let flush t = IO.sync t.log
end
