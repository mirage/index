module Private = struct
  module Fan = Fan
end

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

  val v : ?fresh:bool -> ?readonly:bool -> log_size:int -> string -> t

  val clear : t -> unit

  val find : t -> key -> value

  val mem : t -> key -> bool

  exception Invalid_key_size of key

  exception Invalid_value_size of value

  val replace : t -> key -> value -> unit

  val iter : (key -> value -> unit) -> t -> unit

  val flush : t -> unit

  val close : t -> unit

  val force_merge : t -> key -> value -> unit
end

let may f = function None -> () | Some bf -> f bf

exception RO_not_allowed

let src = Logs.Src.create "index" ~doc:"Index"

module Log = (val Logs.src_log src : Logs.LOG)

module Make (K : Key) (V : Value) (IO : IO) = struct
  type key = K.t

  type value = V.t

  type entry = { key : key; key_hash : int; value : value }

  let entry_size = K.encoded_size + V.encoded_size

  let entry_sizef = float_of_int entry_size

  let entry_sizeL = Int64.of_int entry_size

  exception Invalid_key_size of key

  exception Invalid_value_size of value

  let append_entry io e =
    let encoded_key = K.encode e.key in
    let encoded_value = V.encode e.value in
    if String.length encoded_key <> K.encoded_size then
      raise (Invalid_key_size e.key);
    if String.length encoded_value <> V.encoded_size then
      raise (Invalid_value_size e.value);
    IO.append io encoded_key;
    IO.append io encoded_value

  let decode_entry bytes off =
    let string = Bytes.unsafe_to_string bytes in
    let key = K.decode string off in
    let value = V.decode string (off + K.encoded_size) in
    { key; key_hash = K.hash key; value }

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
    mutable counter : int;
    lock : IO.lock;
  }

  let clear t =
    Log.debug (fun l -> l "clear %S" t.root);
    t.generation <- 0L;
    IO.clear t.log;
    Tbl.clear t.log_mem;
    may
      (fun i ->
        IO.clear i.io;
        IO.close i.io)
      t.index;
    t.index <- None

  let ( // ) = Filename.concat

  let log_path root = root // "index.log"

  let index_path root = root // "index" // "index"

  let lock_path root = root // "index" // ".lock"

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

  type window = { buf : bytes; off : int64 }

  let get_entry io ~window off =
    match window with
    | None ->
        let buf = Bytes.create entry_size in
        let n = IO.read io ~off buf in
        assert (n = entry_size);
        decode_entry buf 0
    | Some w ->
        let off = Int64.(to_int @@ sub off w.off) in
        decode_entry w.buf off

  let with_cache ~v ~clear =
    let roots = Hashtbl.create 0 in
    let f ?(fresh = false) ?(readonly = false) ~log_size root =
      try
        if not (Sys.file_exists root) then (
          Log.debug (fun l ->
              l "[%s] does not exist anymore, cleaning up the fd cache"
                (Filename.basename root));
          Hashtbl.remove roots (root, true);
          Hashtbl.remove roots (root, false);
          raise Not_found );
        let t = Hashtbl.find roots (root, readonly) in
        if IO.valid_fd t.log then (
          Log.debug (fun l -> l "%s found in cache" root);
          t.counter <- t.counter + 1;
          if fresh then clear t;
          t )
        else (
          Hashtbl.remove roots (root, readonly);
          raise Not_found )
      with Not_found ->
        Log.debug (fun l ->
            l "[%s] v fresh=%b readonly=%b" (Filename.basename root) fresh
              readonly);
        let t = v ~fresh ~readonly ~log_size root in
        Hashtbl.add roots (root, readonly) t;
        t
    in
    `Staged f

  let v_no_cache ~fresh ~readonly ~log_size root =
    let lock = IO.lock (lock_path root) in
    let config = { log_size = log_size * entry_size; readonly } in
    let log_path = log_path root in
    let index_path = index_path root in
    let log_mem = Tbl.create 1024 in
    let log = IO.v ~fresh ~readonly ~generation:0L ~fan_size:0L log_path in
    let generation = IO.get_generation log in
    let index =
      if Sys.file_exists index_path then
        let io =
          IO.v ~fresh ~readonly ~generation:0L ~fan_size:0L index_path
        in
        let fan_out = Fan.import ~hash_size:K.hash_size (IO.get_fanout io) in
        Some { fan_out; io }
      else None
    in
    iter_io (fun e -> Tbl.replace log_mem e.key e) log;
    { config; generation; log_mem; root; log; index; counter = 1; lock }

  let (`Staged v) = with_cache ~v:v_no_cache ~clear

  let get_entry_iff_needed ~window io off = function
    | Some e -> e
    | None -> get_entry ~window io off

  let look_around ~window io ~low ~high key h_key off =
    let rec search op curr =
      let off = op curr entry_sizeL in
      if off < low || off > high then raise Not_found
      else
        let e = get_entry ~window io off in
        let h_e = e.key_hash in
        if h_e <> h_key then raise Not_found
        else if K.equal e.key key then e.value
        else search op off
    in
    match search Int64.add off with
    | e -> e
    | exception Not_found -> search Int64.sub off

  let interpolation_search index key : value =
    let hashed_key = K.hash key in
    let low, high = Fan.search index.fan_out hashed_key in
    let rec search steps ~window low high lowest_entry highest_entry =
      if high < low then raise Not_found
      else
        let window =
          match window with
          | Some _ as w -> w
          | None ->
              let len = Int64.(add (sub high low) entry_sizeL) in
              if len <= 4_096L then (
                let buf = Bytes.create (Int64.to_int len) in
                let n = IO.read index.io ~off:low buf in
                assert (n = Bytes.length buf);
                Some { buf; off = low } )
              else None
        in
        let lowest_entry =
          get_entry_iff_needed ~window index.io low lowest_entry
        in
        if high = low then
          if K.equal lowest_entry.key key then lowest_entry.value
          else raise Not_found
        else
          let lowest_hash = lowest_entry.key_hash in
          if lowest_hash > hashed_key then raise Not_found
          else
            let highest_entry =
              get_entry_iff_needed ~window index.io high highest_entry
            in
            let highest_hash = highest_entry.key_hash in
            if highest_hash < hashed_key then raise Not_found
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
              let e = get_entry ~window index.io offL in
              let hashed_e = e.key_hash in
              if hashed_key = hashed_e then
                if K.equal key e.key then e.value
                else
                  look_around ~window ~low ~high index.io key hashed_key offL
              else if hashed_e < hashed_key then
                (search [@tailcall]) (steps + 1) ~window
                  (Int64.add offL entry_sizeL)
                  high None (Some highest_entry)
              else
                (search [@tailcall]) (steps + 1) ~window low
                  (Int64.sub offL entry_sizeL)
                  (Some lowest_entry) None
    in
    if high < 0L then raise Not_found
    else (search [@tailcall]) ~window:None 0 low high None None

  let sync_log t =
    let generation = IO.get_generation t.log in
    let log_offset = IO.offset t.log in
    let new_log_offset = IO.force_offset t.log in
    let add_log_entry e = Tbl.replace t.log_mem e.key e in
    if t.generation <> generation then (
      Tbl.clear t.log_mem;
      iter_io add_log_entry t.log;
      may (fun i -> IO.close i.io) t.index;
      if Int64.equal generation 0L then t.index <- None
      else
        let index_path = index_path t.root in
        let io =
          IO.v ~fresh:false ~readonly:true ~generation ~fan_size:0L index_path
        in
        let fan_out =
          Fan.import ~hash_size:K.encoded_size (IO.get_fanout io)
        in
        t.index <- Some { fan_out; io };
        t.generation <- generation )
    else if log_offset < new_log_offset then
      iter_io add_log_entry t.log ~min:log_offset
    else if log_offset > new_log_offset then assert false

  let find t key =
    Log.debug (fun l -> l "find %a" K.pp key);
    if t.config.readonly then sync_log t;
    let look_on_disk () =
      match Tbl.find t.log_mem key with
      | e -> e.value
      | exception Not_found -> (
          match t.index with
          | Some index -> interpolation_search index key
          | None -> raise Not_found )
    in
    look_on_disk ()

  let mem t key =
    Log.debug (fun l -> l "mem %a" K.pp key);
    match find t key with _ -> true | exception Not_found -> false

  let append_buf_fanout fan_out hash buf_str dst_io =
    Fan.update fan_out hash (IO.offset dst_io);
    IO.append dst_io buf_str

  let append_entry_fanout fan_out entry dst_io =
    Fan.update fan_out entry.key_hash (IO.offset dst_io);
    append_entry dst_io entry

  let rec merge_from_log fan_out log log_i hash_e dst_io =
    if log_i >= Array.length log then log_i
    else
      let v = log.(log_i) in
      if v.key_hash > hash_e then log_i
      else (
        append_entry_fanout fan_out v dst_io;
        (merge_from_log [@tailcall]) fan_out log (log_i + 1) hash_e dst_io )

  let append_remaining_log fan_out log log_i dst_io =
    for log_i = log_i to Array.length log - 1 do
      append_entry_fanout fan_out log.(log_i) dst_io
    done

  (** Merge [log] with [t] into [dst_io].
      [log] must be sorted by key hashes. *)
  let merge_with log index dst_io =
    let entries = 10_000 in
    let buf = Bytes.create (entries * entry_size) in
    let refill off = ignore (IO.read index.io ~off buf) in
    let index_end = IO.offset index.io in
    let fan_out = index.fan_out in
    refill 0L;
    let rec go index_offset buf_offset log_i =
      if index_offset >= index_end then
        append_remaining_log fan_out log log_i dst_io
      else
        let buf_str = Bytes.sub_string buf buf_offset entry_size in
        let index_offset = Int64.add index_offset entry_sizeL in
        let key_e = K.decode buf_str 0 in
        let hash_e = K.hash key_e in
        let log_i = merge_from_log fan_out log log_i hash_e dst_io in
        if
          log_i >= Array.length log
          ||
          let key = log.(log_i).key in
          not (K.equal key key_e)
        then append_buf_fanout fan_out hash_e buf_str dst_io;
        let buf_offset =
          let n = buf_offset + entry_size in
          if n >= Bytes.length buf then (
            refill index_offset;
            0 )
          else n
        in
        (go [@tailcall]) index_offset buf_offset log_i
    in
    (go [@tailcall]) 0L 0 0

  let merge ~witness t =
    Log.debug (fun l -> l "unforced merge %S\n" t.root);
    let tmp_path = t.root // "tmp" // "index" in
    let generation = Int64.succ t.generation in
    let log =
      let compare_entry e e' = compare e.key_hash e'.key_hash in
      let b = Array.make (Tbl.length t.log_mem) witness in
      Tbl.fold
        (fun _ e i ->
          b.(i) <- e;
          i + 1)
        t.log_mem 0
      |> ignore;
      Array.fast_sort compare_entry b;
      b
    in
    let fan_size =
      match t.index with
      | None -> Tbl.length t.log_mem
      | Some index ->
          (Int64.to_int (IO.offset index.io) / entry_size)
          + Tbl.length t.log_mem
    in
    let fan_out = Fan.v ~hash_size:K.hash_size ~entry_size fan_size in
    let tmp =
      IO.v ~readonly:false ~fresh:true ~generation
        ~fan_size:(Int64.of_int (Fan.exported_size fan_out))
        tmp_path
    in
    ( match t.index with
    | None ->
        let io =
          IO.v ~fresh:true ~readonly:false ~generation:0L ~fan_size:0L
            (index_path t.root)
        in
        append_remaining_log fan_out log 0 tmp;
        t.index <- Some { io; fan_out }
    | Some index ->
        let index = { index with fan_out } in
        merge_with log index tmp;
        t.index <- Some index );
    match t.index with
    | None -> assert false
    | Some index ->
        Fan.finalize index.fan_out;
        IO.set_fanout tmp (Fan.export index.fan_out);
        IO.rename ~src:tmp ~dst:index.io;
        IO.clear t.log;
        Tbl.clear t.log_mem;
        IO.set_generation t.log generation;
        t.generation <- generation

  let force_merge t key value =
    Log.debug (fun l -> l "forced merge %S\n" t.root);
    merge ~witness:{ key; key_hash = K.hash key; value } t

  let replace t key value =
    Log.debug (fun l -> l "add %a %a" K.pp key V.pp value);
    if t.config.readonly then raise RO_not_allowed;
    let entry = { key; key_hash = K.hash key; value } in
    append_entry t.log entry;
    Tbl.replace t.log_mem key entry;
    if Int64.compare (IO.offset t.log) (Int64.of_int t.config.log_size) > 0
    then merge ~witness:entry t

  (* XXX: Perform a merge beforehands to ensure duplicates are not hit twice. *)
  let iter f t =
    Tbl.iter (fun _ e -> f e.key e.value) t.log_mem;
    may (fun index -> iter_io (fun e -> f e.key e.value) index.io) t.index

  let flush t = IO.sync t.log

  let close t =
    t.counter <- t.counter - 1;
    if t.counter = 0 then (
      Log.debug (fun l -> l "close %S" t.root);
      if not t.config.readonly then flush t;
      IO.close t.log;
      may (fun i -> IO.close i.io) t.index;
      t.index <- None;
      Tbl.reset t.log_mem;
      IO.unlock t.lock )
end
