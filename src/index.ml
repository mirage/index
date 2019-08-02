module type Key = sig
  type t

  val equal : t -> t -> bool

  val hash : t -> int

  val hash_size : int

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
    string ->
    t

  val clear : t -> unit

  val find_all : t -> key -> value list

  val mem : t -> key -> bool

  val add : t -> key -> value -> unit

  val iter : (key -> value -> unit) -> t -> unit

  val flush : t -> unit
end

let may f = function None -> () | Some bf -> f bf

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

  type config = { log_size : int; readonly : bool }

  type index = { io : IO.t; fan_out : Fan.t }

  type t = {
    config : config;
    root : string;
    mutable generation : int64;
    mutable index : index option;
    log : IO.t;
    log_mem : entry Tbl.t;
    entries : key Bloomf.t option;
  }

  let clear t =
    Log.debug (fun l -> l "clear %S" t.root);
    t.generation <- 0L;
    IO.clear t.log;
    may Bloomf.clear t.entries;
    Tbl.clear t.log_mem;
    may (fun i -> IO.clear i.io) t.index;
    t.index <- None

  let ( // ) = Filename.concat

  let log_path root = root // "index.log"

  let index_path root = root // "index" // "index"

  let page_size = Int64.mul entry_sizeL 1_000L

  let iter_io_off ?(min = 0L) ?max f io =
    let max = match max with None -> IO.offset io | Some m -> m in
    let rec aux offset =
      let remaining = Int64.sub max offset in
      if remaining <= 0L then ()
      else
        let size = Stdlib.min remaining page_size in
        let raw = Bytes.create (Int64.to_int size) in
        let n = IO.read io ~off:offset raw in
        let rec read_page page off =
          if off = n then ()
          else
            let entry = decode_entry page off in
            f Int64.(add (of_int off) offset) entry;
            (read_page [@tailcall]) page (off + entry_size)
        in
        read_page raw 0;
        (aux [@tailcall]) Int64.(add offset page_size)
    in
    (aux [@tailcall]) min

  let iter_io ?min ?max f io = iter_io_off ?min ?max (fun _ e -> f e) io

  let get_entry io off =
    let buf = Bytes.create entry_size in
    let _ = IO.read io ~off buf in
    decode_entry buf 0

  let with_cache ~v ~clear =
    let roots = Hashtbl.create 0 in
    let f ?(fresh = false) ?(readonly = false) ?(shared = true) ~log_size root
        =
      if not shared then (
        Log.debug (fun l ->
            l "[%s] v fresh=%b shared=%b readonly=%b" (Filename.basename root)
              fresh shared readonly);
        v ~fresh ~readonly ~log_size root )
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
          let t = v ~fresh ~readonly ~log_size root in
          Hashtbl.add roots root t;
          t
    in
    `Staged f

  let v_no_cache ~fresh ~readonly ~log_size root =
    let config = { log_size = log_size * entry_size; readonly } in
    let log_path = log_path root in
    let index_path = index_path root in
    let entries =
      if readonly then None
      else Some (Bloomf.create ~error_rate:0.01 100_000_000)
    in
    let log_mem = Tbl.create 1024 in
    let log = IO.v ~fresh ~readonly ~generation:0L log_path in
    let generation = IO.get_generation log in
    let index =
      if Sys.file_exists index_path then (
        let io = IO.v ~fresh ~readonly ~generation:0L index_path in
        let fan_out_size = Int64.to_int (IO.offset io) / entry_size in
        let fan_out = Fan.v ~hash_size:K.hash_size ~entry_size fan_out_size in
        iter_io_off
          (fun off e ->
            let hash = K.hash e.key in
            Fan.update fan_out hash off;
            may (fun bf -> Bloomf.add bf e.key) entries)
          io;
        Fan.finalize fan_out;
        Some { fan_out; io } )
      else None
    in
    iter_io
      (fun e ->
        Tbl.add log_mem e.key e;
        may (fun bf -> Bloomf.add bf e.key) entries)
      log;
    { config; generation; log_mem; root; log; index; entries }

  let (`Staged v) = with_cache ~v:v_no_cache ~clear

  let get_entry_iff_needed io off = function
    | Some e -> e
    | None -> get_entry io off

  let look_around io init key h_key off =
    let rec search acc op curr =
      let off = op curr entry_sizeL in
      if off < 0L || off >= IO.offset io then acc
      else
        let e = get_entry io off in
        let h_e = K.hash e.key in
        if h_e <> h_key then acc
        else
          let new_acc = if K.equal e.key key then e.value :: acc else acc in
          search new_acc op off
    in
    let before = search init Int64.add off in
    search before Int64.sub off

  let interpolation_search index key =
    let hashed_key = K.hash key in
    let low, high = Fan.search index.fan_out hashed_key in
    let rec search low high lowest_entry highest_entry =
      if high < low then []
      else
        let lowest_entry = get_entry_iff_needed index.io low lowest_entry in
        if high = low then
          if K.equal lowest_entry.key key then [ lowest_entry.value ] else []
        else
          let lowest_hash = K.hash lowest_entry.key in
          if lowest_hash > hashed_key then []
          else
            let highest_entry =
              get_entry_iff_needed index.io high highest_entry
            in
            let highest_hash = K.hash highest_entry.key in
            if highest_hash < hashed_key then []
            else
              let lowest_hashf = float_of_int lowest_hash in
              let highest_hashf = float_of_int highest_hash in
              let hashed_keyf = float_of_int hashed_key in
              let lowf = Int64.to_float low in
              let highf = Int64.to_float high in
              let doff =
                floor
                  ( (highf -. lowf)
                  *. (hashed_keyf -. lowest_hashf)
                  /. (highest_hashf -. lowest_hashf) )
              in
              let off = lowf +. doff -. mod_float doff entry_sizef in
              let offL = Int64.of_float off in
              let e = get_entry index.io offL in
              let hashed_e = K.hash e.key in
              if hashed_key = hashed_e then
                let init = if K.equal key e.key then [ e.value ] else [] in
                look_around index.io init key hashed_key offL
              else if hashed_e < hashed_key then
                (search [@tailcall])
                  (Int64.add offL entry_sizeL)
                  high None (Some highest_entry)
              else
                (search [@tailcall]) low
                  (Int64.sub offL entry_sizeL)
                  (Some lowest_entry) None
    in
    if high < 0L then [] else (search [@tailcall]) low high None None

  let sync_log t =
    let generation = IO.get_generation t.log in
    let log_offset = IO.offset t.log in
    let new_log_offset = IO.force_offset t.log in
    let add_log_entry e =
      Tbl.add t.log_mem e.key e;
      may (fun bf -> Bloomf.add bf e.key) t.entries
    in
    if t.generation <> generation then (
      Tbl.clear t.log_mem;
      iter_io add_log_entry t.log;
      let index_path = index_path t.root in
      let io = IO.v ~fresh:false ~readonly:true ~generation index_path in
      let _ = IO.force_offset io in
      let io_off = IO.force_offset io in
      let fan_out_size =
        Tbl.length t.log_mem + (Int64.to_int io_off / entry_size)
      in
      let fan_out = Fan.v ~hash_size:K.hash_size ~entry_size fan_out_size in
      iter_io_off
        (fun off e ->
          let hash = K.hash e.key in
          Fan.update fan_out hash off)
        io;
      Fan.finalize fan_out;
      t.index <- Some { fan_out; io };
      t.generation <- generation )
    else if log_offset < new_log_offset then
      iter_io add_log_entry t.log ~min:log_offset
    else if log_offset > new_log_offset then assert false

  let find_all t key =
    Log.debug (fun l -> l "find %a" K.pp key);
    if t.config.readonly then sync_log t;
    let look_on_disk () =
      let in_index =
        match t.index with
        | None -> []
        | Some index -> interpolation_search index key
      in
      let in_log = List.map (fun e -> e.value) (Tbl.find_all t.log_mem key) in
      in_index @ in_log
    in
    match t.entries with
    | None -> look_on_disk ()
    | Some bf -> if not (Bloomf.mem bf key) then [] else look_on_disk ()

  let mem t key =
    Log.debug (fun l -> l "mem %a" K.pp key);
    match find_all t key with [] -> false | _ -> true

  let append_entry_fanout fan_out h io e =
    Fan.update fan_out h (IO.offset io);
    append_entry io e

  let merge_with log index tmp =
    let offset = ref 0L in
    let get_index_entry = function
      | Some e -> Some e
      | None ->
          if !offset >= IO.offset index.io then None
          else
            let e = get_entry index.io !offset in
            offset := Int64.add !offset entry_sizeL;
            Some e
    in
    let fan_out = index.fan_out in
    let rec go last_read l =
      match get_index_entry last_read with
      | None ->
          List.iter
            (fun v ->
              let hashed_v = K.hash v.key in
              append_entry_fanout fan_out hashed_v tmp v)
            l
      | Some e -> (
          let hashed_e = K.hash e.key in
          match l with
          | v :: r ->
              let last, rst =
                let hashed_v = K.hash v.key in
                if hashed_e = hashed_v then (
                  append_entry_fanout fan_out hashed_e tmp e;
                  append_entry_fanout fan_out hashed_v tmp v;
                  (None, r) )
                else if hashed_e < hashed_v then (
                  append_entry_fanout fan_out hashed_e tmp e;
                  (None, l) )
                else (
                  append_entry_fanout fan_out hashed_v tmp v;
                  (Some e, r) )
              in
              if !offset >= IO.offset index.io && last = None then
                List.iter
                  (fun v ->
                    let hashed_v = K.hash v.key in
                    append_entry_fanout fan_out hashed_v tmp v)
                  rst
              else (go [@tailcall]) last rst
          | [] ->
              append_entry_fanout fan_out hashed_e tmp e;
              iter_io
                (fun e ->
                  let hashed_e = K.hash e.key in
                  append_entry_fanout fan_out hashed_e tmp e)
                index.io ~min:!offset )
    in
    (go [@tailcall]) None log

  module EntrySet = Set.Make (struct
    type t = entry

    let compare e e' =
      let c = compare (K.hash e.key) (K.hash e'.key) in
      if c = 0 then 1 else c
  end)

  let merge t =
    Log.debug (fun l -> l "merge %S" t.root);
    let tmp_path = t.root // "tmp" // "index" in
    let generation = Int64.succ t.generation in
    let tmp = IO.v ~readonly:false ~fresh:true ~generation tmp_path in
    let log =
      Tbl.fold (fun _ e acc -> EntrySet.add e acc) t.log_mem EntrySet.empty
      |> EntrySet.elements
    in
    ( match t.index with
    | None ->
        let fan_out_size = Tbl.length t.log_mem in
        let fan_out = Fan.v ~hash_size:K.hash_size ~entry_size fan_out_size in
        let io =
          IO.v ~fresh:true ~readonly:false ~generation:0L (index_path t.root)
        in
        List.iter
          (fun v ->
            let hashed_v = K.hash v.key in
            append_entry_fanout fan_out hashed_v tmp v)
          log;
        t.index <- Some { io; fan_out }
    | Some index ->
        let fan_out_size =
          (Int64.to_int (IO.offset index.io) / entry_size)
          + Tbl.length t.log_mem
        in
        let fan_out = Fan.v ~hash_size:K.hash_size ~entry_size fan_out_size in
        let index = { index with fan_out } in
        merge_with log index tmp;
        t.index <- Some index );
    match t.index with
    | None -> assert false
    | Some index ->
        IO.rename ~src:tmp ~dst:index.io;
        Fan.finalize index.fan_out;
        IO.clear t.log;
        Tbl.clear t.log_mem;
        IO.set_generation t.log generation;
        t.generation <- generation

  let add t key value =
    Log.debug (fun l -> l "add %a %a" K.pp key V.pp value);
    if t.config.readonly then raise RO_Not_Allowed;
    let entry = { key; value } in
    append_entry t.log entry;
    Tbl.add t.log_mem key entry;
    may (fun bf -> Bloomf.add bf key) t.entries;
    if Int64.compare (IO.offset t.log) (Int64.of_int t.config.log_size) > 0
    then merge t

  (* XXX: Perform a merge beforehands to ensure duplicates are not hit twice. *)
  let iter f t =
    Tbl.iter (fun _ e -> f e.key e.value) t.log_mem;
    may (fun index -> iter_io (fun e -> f e.key e.value) index.io) t.index

  let flush t = IO.sync t.log
end
