(* The MIT License

Copyright (c) 2019 Craig Ferguson <craig@tarides.com>
                   Thomas Gazagnaire <thomas@tarides.com>
                   Ioana Cristescu <ioana@tarides.com>
                   Clément Pascutto <clement@tarides.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software. *)

include Index_intf
module Stats = Stats
module Cache = Cache
module Checks = Checks

module Key = struct
  module type S = Key

  module String_fixed = Data.String_fixed
end

module Value = struct
  module type S = Value

  module String_fixed = Data.String_fixed
end

let may f = function None -> () | Some bf -> f bf

let assert_and_get = function None -> assert false | Some e -> e

exception RO_not_allowed

exception RW_not_allowed

exception Closed

module Make_private
    (K : Key)
    (V : Value)
    (IO : Io.S)
    (Semaphore : SEMAPHORE)
    (Thread : THREAD)
    (Cache : Cache.S) =
struct
  type 'a async = 'a Thread.t

  let await = Thread.await

  type key = K.t

  type value = V.t

  let pp_key = Repr.pp K.t

  let pp_value = Repr.pp V.t

  module Entry = struct
    include Data.Entry.Make (K) (V)
    module Key = K
    module Value = V

    let to_key { key; _ } = key

    let to_value { value; _ } = value
  end

  let entry_sizeL = Int64.of_int Entry.encoded_size

  module Tbl = Hashtbl.Make (K)

  module IO = struct
    include Io.Extend (IO)

    let page_size = Int64.mul entry_sizeL 1_000L

    let iter ?min ?max f =
      iter ~page_size ?min ?max (fun ~off ~buf ~buf_off ->
          let entry = Entry.decode buf buf_off in
          f off entry;
          Entry.encoded_size)
  end

  let iter_io ?min ?max f = IO.iter ?min ?max (fun _ -> f)

  type throttle = [ `Overcommit_memory | `Block_writes ]

  type config = {
    log_size : int;
    readonly : bool;
    fresh : bool;
    throttle : throttle;
    flush_callback : unit -> unit;
  }

  type index = { io : IO.t; fan_out : [ `Read ] Fan.t }

  type log = { io : IO.t; mem : value Tbl.t }

  type instance = {
    config : config;
    root : string;
    mutable generation : int64;
    mutable index : index option;
    mutable log : log option;
    mutable log_async : log option;
    mutable open_instances : int;
    writer_lock : IO.Lock.t option;
    merge_lock : Semaphore.t;
    rename_lock : Semaphore.t;
    mutable pending_cancel : bool;
        (** Signal the merge thread to terminate prematurely *)
  }

  include Private_types

  let check_pending_cancel instance =
    match instance.pending_cancel with true -> `Abort | false -> `Continue

  type t = instance option ref

  let check_open t =
    match !t with Some instance -> instance | None -> raise Closed

  let clear' ~hook t =
    let t = check_open t in
    Log.debug (fun l -> l "clear %S" t.root);
    if t.config.readonly then raise RO_not_allowed;
    t.pending_cancel <- true;
    hook `Abort_signalled;
    Semaphore.with_acquire "clear" t.merge_lock (fun () ->
        t.pending_cancel <- false;
        t.generation <- Int64.succ t.generation;
        let log = assert_and_get t.log in
        IO.clear ~generation:t.generation log.io;
        Tbl.clear log.mem;
        may
          (fun l ->
            IO.clear ~generation:t.generation l.io;
            IO.close l.io)
          t.log_async;
        may
          (fun (i : index) ->
            IO.clear ~generation:t.generation i.io;
            IO.close i.io)
          t.index;
        t.index <- None;
        t.log_async <- None)

  let clear = clear' ~hook:(fun _ -> ())

  let flush_instance ?no_async ?no_callback ?(with_fsync = false) instance =
    Log.debug (fun l ->
        l "[%s] flushing instance" (Filename.basename instance.root));
    if instance.config.readonly then raise RO_not_allowed;
    instance.log
    |> may (fun log ->
           Log.debug (fun l ->
               l "[%s] flushing log" (Filename.basename instance.root));
           IO.flush ?no_callback ~with_fsync log.io);

    match (no_async, instance.log_async) with
    | Some (), _ | None, None -> ()
    | None, Some log ->
        Log.debug (fun l ->
            l "[%s] flushing log_async" (Filename.basename instance.root));
        IO.flush ?no_callback ~with_fsync log.io

  let flush ?no_callback ?(with_fsync = false) t =
    let t = check_open t in
    Log.debug (fun l -> l "[%s] flush" (Filename.basename t.root));
    Semaphore.with_acquire "flush" t.rename_lock (fun () ->
        flush_instance ?no_callback ~with_fsync t)

  module IOArray = Io_array.Make (IO) (Entry)

  module Search =
    Search.Make (Entry) (IOArray)
      (struct
        type t = int

        module Entry = Entry

        let compare : int -> int -> int = compare

        let of_entry e = e.Entry.key_hash

        let of_key = K.hash

        let linear_interpolate ~low:(low_index, low_metric)
            ~high:(high_index, high_metric) key_metric =
          let low_in = float_of_int low_metric in
          let high_in = float_of_int high_metric in
          let target_in = float_of_int key_metric in
          let low_out = Int64.to_float low_index in
          let high_out = Int64.to_float high_index in
          (* Fractional position of [target_in] along the line from [low_in] to [high_in] *)
          let proportion = (target_in -. low_in) /. (high_in -. low_in) in
          (* Convert fractional position to position in output space *)
          let position = low_out +. (proportion *. (high_out -. low_out)) in
          let rounded = ceil (position -. 0.5) +. 0.5 in
          Int64.of_float rounded
      end)

  let try_load_log t path =
    Log.debug (fun l ->
        l "[%s] checking on-disk %s file" (Filename.basename t.root)
          (Filename.basename path));
    if IO.exists path then (
      let io =
        IO.v ~fresh:false ~readonly:true ~generation:0L ~fan_size:0L path
      in
      let mem = Tbl.create 0 in
      iter_io (fun e -> Tbl.replace mem e.key e.value) io;
      Some { io; mem })
    else None

  let sync_log_entries ?min log =
    let add_log_entry (e : Entry.t) = Tbl.replace log.mem e.key e.value in
    if min = None then Tbl.clear log.mem;
    iter_io ?min add_log_entry log.io

  let sync_log_async t =
    match t.log_async with
    | None -> t.log_async <- try_load_log t (Layout.log_async ~root:t.root)
    | Some log ->
        let offset = IO.offset log.io in
        let h = IO.Header.get log.io in
        if t.generation <> h.generation then sync_log_entries log
        else if offset < h.offset then sync_log_entries ~min:offset log
        else if offset > h.offset then assert false

  let sync_index ~generation t =
    may (fun (i : index) -> IO.close i.io) t.index;
    let index_path = Layout.data ~root:t.root in
    if not (IO.exists index_path) then t.index <- None
    else
      let io =
        IO.v ~fresh:false ~readonly:true ~generation ~fan_size:0L index_path
      in
      let fan_out = Fan.import ~hash_size:K.hash_size (IO.get_fanout io) in
      if IO.offset io = 0L then t.index <- None
      else t.index <- Some { fan_out; io }

  let sync_log ?(hook = fun _ -> ()) t =
    Log.debug (fun l ->
        l "[%s] checking for changes on disk" (Filename.basename t.root));
    (match t.log with
    | None -> t.log <- try_load_log t (Layout.log ~root:t.root)
    | Some _ -> ());
    (* there is a race between sync and merge:

       - At the end of the merge, the entries in log_async are copied
       into log. [merge] starts by calling IO.Header.set(log) with a
       new generation number, copies all the entries and then clear
       log_async.

       - so here we need to make sure we do the same thing in reverse:
       start by syncing [log_async], then read [log]'s headers. At
       worse, [log_async] and [log] might contain duplicated entries,
       but we won't miss any. These entries will be added to [log.mem]
       using Tbl.replace where they will be deduplicated. *)
    sync_log_async t;
    match t.log with
    | None -> ()
    | Some log ->
        let log_offset = IO.offset log.io in
        hook `Before_offset_read;
        let h = IO.Header.get log.io in
        hook `After_offset_read;
        if t.generation <> h.generation then (
          Log.debug (fun l ->
              l "[%s] generation has changed, reading log and index from disk"
                (Filename.basename t.root));
          t.generation <- h.generation;
          sync_log_entries log;
          sync_index ~generation:h.generation t)
        else if log_offset < h.offset then (
          Log.debug (fun l ->
              l "[%s] new entries detected, reading log from disk"
                (Filename.basename t.root));
          sync_log_entries ~min:log_offset log)
        else
          Log.debug (fun l ->
              l "[%s] no changes detected" (Filename.basename t.root))

  let v_no_cache ?(flush_callback = fun () -> ()) ~throttle ~fresh ~readonly
      ~log_size root =
    Log.debug (fun l ->
        l "[%s] not found in cache, creating a new instance"
          (Filename.basename root));
    let writer_lock =
      if not readonly then Some (IO.Lock.lock (Layout.lock ~root)) else None
    in
    let config =
      {
        log_size = log_size * Entry.encoded_size;
        readonly;
        fresh;
        throttle;
        flush_callback;
      }
    in
    let log_path = Layout.log ~root in
    let log =
      if readonly then if fresh then raise RO_not_allowed else None
      else
        let io =
          IO.v ~flush_callback ~fresh ~readonly ~generation:0L ~fan_size:0L
            log_path
        in
        let entries = Int64.div (IO.offset io) entry_sizeL in
        Log.debug (fun l ->
            l "[%s] log file detected. Loading %Ld entries"
              (Filename.basename root) entries);
        let mem = Tbl.create (Int64.to_int entries) in
        iter_io (fun e -> Tbl.replace mem e.key e.value) io;
        Some { io; mem }
    in
    let generation =
      match log with None -> 0L | Some log -> IO.get_generation log.io
    in
    let log_async_path = Layout.log_async ~root in
    (* If we are in readonly mode, the log_async will be read during sync_log so
       there is no need to do it here. *)
    if (not readonly) && IO.exists log_async_path then (
      let io =
        IO.v ~flush_callback ~fresh ~readonly:false ~generation:0L ~fan_size:0L
          log_async_path
      in
      let entries = Int64.div (IO.offset io) entry_sizeL in
      Log.debug (fun l ->
          l "[%s] log_async file detected. Loading %Ld entries"
            (Filename.basename root) entries);
      (* If we are not in fresh mode, we move the contents of log_async to
         log. *)
      if not fresh then
        may
          (fun log ->
            let append_io = IO.append log.io in
            iter_io
              (fun e ->
                Tbl.replace log.mem e.key e.value;
                Entry.encode e append_io)
              io;
            IO.flush log.io;
            IO.clear ~generation io)
          log;
      IO.close io);
    let index =
      let index_path = Layout.data ~root in
      if IO.exists index_path then
        let io =
          (* NOTE: No [flush_callback] on the Index IO as we maintain the
             invariant that any bindings it contains were previously persisted
             in either [log] or [log_async]. *)
          IO.v ?flush_callback:None ~fresh ~readonly ~generation ~fan_size:0L
            index_path
        in
        let entries = Int64.div (IO.offset io) entry_sizeL in
        if entries = 0L then None
        else (
          Log.debug (fun l ->
              l "[%s] index file detected. Loading %Ld entries"
                (Filename.basename root) entries);
          let fan_out = Fan.import ~hash_size:K.hash_size (IO.get_fanout io) in
          Some { fan_out; io })
      else (
        Log.debug (fun l ->
            l "[%s] no index file detected." (Filename.basename root));
        None)
    in
    {
      config;
      generation;
      log;
      log_async = None;
      root;
      index;
      open_instances = 1;
      merge_lock = Semaphore.make true;
      rename_lock = Semaphore.make true;
      writer_lock;
      pending_cancel = false;
    }

  type cache = (string * bool, instance) Cache.t

  let empty_cache = Cache.create

  let v ?(flush_callback = fun () -> ()) ?(cache = empty_cache ())
      ?(fresh = false) ?(readonly = false) ?(throttle = `Block_writes) ~log_size
      root =
    let new_instance () =
      let instance =
        v_no_cache ~flush_callback ~fresh ~readonly ~log_size ~throttle root
      in
      if readonly then sync_log instance;
      Cache.add cache (root, readonly) instance;
      ref (Some instance)
    in
    Log.info (fun l ->
        l "[%s] v fresh=%b readonly=%b log_size=%d" (Filename.basename root)
          fresh readonly log_size);
    match (Cache.find cache (root, readonly), IO.exists (Layout.log ~root)) with
    | None, _ -> new_instance ()
    | Some _, false ->
        Log.debug (fun l ->
            l "[%s] does not exist anymore, cleaning up the fd cache"
              (Filename.basename root));
        Cache.remove cache (root, true);
        Cache.remove cache (root, false);
        new_instance ()
    | Some t, true -> (
        match t.open_instances with
        | 0 ->
            Cache.remove cache (root, readonly);
            new_instance ()
        | _ ->
            Log.debug (fun l ->
                l "[%s] found in cache" (Filename.basename root));
            t.open_instances <- t.open_instances + 1;
            if readonly then sync_log t;
            let t = ref (Some t) in
            if fresh then clear t;
            t)

  let interpolation_search index key =
    let hashed_key = K.hash key in
    let low_bytes, high_bytes = Fan.search index.fan_out hashed_key in
    let low, high =
      Int64.(div low_bytes entry_sizeL, div high_bytes entry_sizeL)
    in
    Search.interpolation_search (IOArray.v index.io) key ~low ~high

  let find_instance t key =
    let find_if_exists ~name ~find db =
      match db with
      | None -> raise Not_found
      | Some e ->
          let ans = find e key in
          Log.debug (fun l ->
              l "[%s] found in %s" (Filename.basename t.root) name);
          ans
    in
    let find_log_async () =
      find_if_exists ~name:"log_async"
        ~find:(fun log -> Tbl.find log.mem)
        t.log_async
    in
    let find_log () =
      find_if_exists ~name:"log" ~find:(fun log -> Tbl.find log.mem) t.log
    in
    let find_index () =
      find_if_exists ~name:"index" ~find:interpolation_search t.index
    in
    Semaphore.with_acquire "find_instance" t.rename_lock @@ fun () ->
    match find_log_async () with
    | e -> e
    | exception Not_found -> (
        match find_log () with e -> e | exception Not_found -> find_index ())

  let find t key =
    let t = check_open t in
    Log.debug (fun l -> l "[%s] find %a" (Filename.basename t.root) pp_key key);
    find_instance t key

  let mem t key =
    let t = check_open t in
    Log.debug (fun l -> l "[%s] mem %a" (Filename.basename t.root) pp_key key);
    match find_instance t key with _ -> true | exception Not_found -> false

  let append_buf_fanout fan_out hash buf_str dst_io =
    Fan.update fan_out hash (IO.offset dst_io);
    IO.append dst_io buf_str

  let append_entry_fanout fan_out entry dst_io =
    Fan.update fan_out entry.Entry.key_hash (IO.offset dst_io);
    Entry.encode entry (IO.append dst_io)

  let rec merge_from_log fan_out log log_i hash_e dst_io =
    if log_i >= Array.length log then log_i
    else
      let v = log.(log_i) in
      if v.Entry.key_hash >= hash_e then log_i
      else (
        append_entry_fanout fan_out v dst_io;
        (merge_from_log [@tailcall]) fan_out log (log_i + 1) hash_e dst_io)

  let append_remaining_log fan_out log log_i dst_io =
    for log_i = log_i to Array.length log - 1 do
      append_entry_fanout fan_out log.(log_i) dst_io
    done

  (* Merge [log] with [index] into [dst_io], ignoring bindings that do not
     satisfy [filter (k, v)]. [log] must be sorted by key hashes. *)
  let merge_with ~hook ~yield ~filter log index_io fan_out dst_io =
    let entries = 10_000 in
    let len = entries * Entry.encoded_size in
    let buf = Bytes.create len in
    let refill off = ignore (IO.read index_io ~off ~len buf) in
    let index_end = IO.offset index_io in
    refill 0L;
    let rec go first_entry index_offset buf_offset log_i =
      if index_offset >= index_end then (
        append_remaining_log fan_out log log_i dst_io;
        `Completed)
      else
        let index_offset = Int64.add index_offset entry_sizeL in
        let e = Entry.decode (Bytes.unsafe_to_string buf) buf_offset in
        let log_i = merge_from_log fan_out log log_i e.key_hash dst_io in
        match yield () with
        | `Abort -> `Aborted
        | `Continue ->
            Thread.yield ();
            (if
             (log_i >= Array.length log
             ||
             let key = log.(log_i).key in
             not K.(equal key e.key))
             && filter (e.key, e.value)
            then
             let buf_str = Bytes.sub buf buf_offset Entry.encoded_size in
             append_buf_fanout fan_out e.key_hash
               (Bytes.unsafe_to_string buf_str)
               dst_io);
            if first_entry then hook `After_first_entry;
            let buf_offset =
              let n = buf_offset + Entry.encoded_size in
              if n >= Bytes.length buf then (
                refill index_offset;
                0)
              else n
            in
            (go [@tailcall]) false index_offset buf_offset log_i
    in
    (go [@tailcall]) true 0L 0 0

  let merge_counter =
    let n = ref 0 in
    fun () ->
      incr n;
      !n

  let merge' ?(blocking = false) ?(filter = fun _ -> true) ?(hook = fun _ -> ())
      ~witness ?(force = false) t =
    let yield () = check_pending_cancel t in
    let counter = Mtime_clock.counter () in
    let merge_id = merge_counter () in
    let msg = Fmt.strf "merge { id=%d }" merge_id in
    Semaphore.acquire msg t.merge_lock;
    let merge_lock_wait = Mtime_clock.count counter in
    Log.info (fun l ->
        let pp_forced ppf () = if force then Fmt.string ppf "; force=true" in
        l "[%s] merge started { id=%d%a }" (Filename.basename t.root) merge_id
          pp_forced ());
    Stats.incr_nb_merge ();
    let log_async =
      let io =
        let log_async_path = Layout.log_async ~root:t.root in
        IO.v ~flush_callback:t.config.flush_callback ~fresh:true ~readonly:false
          ~generation:(Int64.succ t.generation) ~fan_size:0L log_async_path
      in
      let mem = Tbl.create 0 in
      { io; mem }
    in
    t.log_async <- Some log_async;
    (* NOTE: We flush [log] {i after} enabling [log_async] to ensure that no
       unflushed bindings make it into [log] before being merged into the index.
       This satisfies the invariant that all bindings are {i first} persisted in
       a log, so that the [index] IO doesn't need to trigger the
       [flush_callback]. *)
    flush_instance ~no_async:() ~with_fsync:true t;

    let go () =
      hook `Before;
      let log = assert_and_get t.log in
      let generation = Int64.succ t.generation in
      let log_array =
        let compare_entry (e : Entry.t) (e' : Entry.t) =
          compare e.key_hash e'.key_hash
        in
        Tbl.filter_map_inplace
          (fun key value -> if filter (key, value) then Some value else None)
          log.mem;
        let b = Array.make (Tbl.length log.mem) witness in
        Tbl.fold
          (fun key value i ->
            b.(i) <- Entry.v key value;
            i + 1)
          log.mem 0
        |> ignore;
        Array.fast_sort compare_entry b;
        b
      in
      let fan_size =
        match t.index with
        | None -> Tbl.length log.mem
        | Some index ->
            (Int64.to_int (IO.offset index.io) / Entry.encoded_size)
            + Array.length log_array
      in
      let fan_out =
        Fan.v ~hash_size:K.hash_size ~entry_size:Entry.encoded_size fan_size
      in
      let merge =
        let merge_path = Layout.merge ~root:t.root in
        IO.v ~fresh:true ~readonly:false ~generation
          ~fan_size:(Int64.of_int (Fan.exported_size fan_out))
          merge_path
      in
      let merge_result : [ `Index_io of IO.t | `Aborted ] =
        match t.index with
        | None -> (
            match yield () with
            | `Abort -> `Aborted
            | `Continue ->
                let io =
                  IO.v ~fresh:true ~readonly:false ~generation ~fan_size:0L
                    (Layout.data ~root:t.root)
                in
                append_remaining_log fan_out log_array 0 merge;
                `Index_io io)
        | Some index -> (
            match
              merge_with ~hook ~yield ~filter log_array index.io fan_out merge
            with
            | `Completed -> `Index_io index.io
            | `Aborted -> `Aborted)
      in
      let cleanup_result, rename_lock_wait =
        match merge_result with
        | `Aborted -> (`Aborted, Mtime.Span.zero)
        | `Index_io io ->
            let fan_out = Fan.finalize fan_out in
            let index = { io; fan_out } in
            IO.set_fanout merge (Fan.export index.fan_out);
            let rename_lock_wait =
              let before_rename_lock = Mtime_clock.count counter in
              Semaphore.with_acquire "merge-rename" t.rename_lock (fun () ->
                  let after_rename_lock = Mtime_clock.count counter in
                  IO.rename ~src:merge ~dst:index.io;
                  t.index <- Some index;
                  t.generation <- generation;
                  IO.clear ~generation log.io;
                  Tbl.clear log.mem;
                  hook `After_clear;
                  let log_async = assert_and_get t.log_async in
                  let append_io = IO.append log.io in
                  Tbl.iter
                    (fun key value ->
                      Tbl.replace log.mem key value;
                      Entry.encode' key value append_io)
                    log_async.mem;
                  (* NOTE: It {i may} not be necessary to trigger the
                     [flush_callback] here. If the instance has been recently
                     flushed (or [log_async] just reached the [auto_flush_limit]),
                     we're just moving already-persisted values around. However, we
                     trigger the callback anyway for simplicity. *)
                  (* `fsync` is necessary, since bindings in `log_async` may have
                     been explicitely `fsync`ed during the merge, so we need to
                     maintain their durability. *)
                  (try IO.flush ~with_fsync:true log.io
                   with exn ->
                     Semaphore.release t.merge_lock;
                     raise exn);
                  IO.clear ~generation:(Int64.succ generation) log_async.io;
                  (* log_async.mem does not need to be cleared as we are discarding
                     it. *)
                  t.log_async <- None;
                  Mtime.Span.abs_diff after_rename_lock before_rename_lock)
            in
            IO.close log_async.io;
            hook `After;
            (`Completed, rename_lock_wait)
      in
      let total_duration = Mtime_clock.count counter in
      let merge_duration = Mtime.Span.abs_diff total_duration merge_lock_wait in
      Semaphore.release t.merge_lock;
      Stats.add_merge_duration merge_duration;
      Log.info (fun l ->
          let action =
            match cleanup_result with
            | `Aborted -> "aborted"
            | `Completed -> "completed"
          in
          l
            "[%s] merge %s { id=%d; total-duration=%a; merge-duration=%a; \
             merge-lock=%a; rename-lock=%a }"
            (Filename.basename t.root) action merge_id Mtime.Span.pp
            total_duration Mtime.Span.pp merge_duration Mtime.Span.pp
            merge_lock_wait Mtime.Span.pp rename_lock_wait);
      cleanup_result
    in
    if blocking then go () |> Thread.return else Thread.async go

  exception Found of Entry.t

  let get_witness t =
    match t.log with
    | None -> None
    | Some log -> (
        match
          Tbl.iter (fun key value -> raise (Found (Entry.v key value))) log.mem
        with
        | exception Found e -> Some e
        | () -> (
            match t.index with
            | None -> None
            | Some index ->
                let buf = Bytes.create Entry.encoded_size in
                let n = IO.read index.io ~off:0L ~len:Entry.encoded_size buf in
                assert (n = Entry.encoded_size);
                Some (Entry.decode (Bytes.unsafe_to_string buf) 0)))

  let try_merge_aux ?hook ?(force = false) t =
    let t = check_open t in
    let witness =
      Semaphore.with_acquire "witness" t.rename_lock (fun () -> get_witness t)
    in
    match witness with
    | None ->
        Log.debug (fun l -> l "[%s] index is empty" (Filename.basename t.root));
        Thread.return `Completed
    | Some witness -> (
        match t.log with
        | None ->
            Log.debug (fun l ->
                l "[%s] log is empty" (Filename.basename t.root));
            Thread.return `Completed
        | Some log ->
            if
              force
              || Int64.compare (IO.offset log.io)
                   (Int64.of_int t.config.log_size)
                 > 0
            then merge' ~force ?hook ~witness t
            else Thread.return `Completed)

  let merge t = ignore (try_merge_aux ?hook:None ~force:true t : _ async)

  let try_merge t = ignore (try_merge_aux ?hook:None ~force:false t : _ async)

  (** [t.merge_lock] is used to detect an ongoing merge. Other operations can
      take this lock, but as they are not async, we consider this to be a good
      enough approximations. *)
  let instance_is_merging t = Semaphore.is_held t.merge_lock

  let is_merging t =
    let t = check_open t in
    if t.config.readonly then raise RO_not_allowed;
    instance_is_merging t

  let replace' ?hook ?(overcommit = false) t key value =
    let t = check_open t in
    Stats.incr_nb_replace ();
    Log.debug (fun l ->
        l "[%s] replace %a %a" (Filename.basename t.root) pp_key key pp_value
          value);
    if t.config.readonly then raise RO_not_allowed;
    let log_limit_reached =
      Semaphore.with_acquire "replace" t.rename_lock (fun () ->
          let log =
            match t.log_async with
            | Some log -> log
            | None -> assert_and_get t.log
          in
          Entry.encode' key value (IO.append log.io);
          Tbl.replace log.mem key value;
          Int64.compare (IO.offset log.io) (Int64.of_int t.config.log_size) > 0)
    in
    if log_limit_reached && not overcommit then
      let is_merging = instance_is_merging t in
      match (t.config.throttle, is_merging) with
      | `Overcommit_memory, true ->
          (* Merging now would block on completion of the ongoing merge *)
          None
      | `Overcommit_memory, false | `Block_writes, _ ->
          let hook = hook |> Option.map (fun f stage -> f (`Merge stage)) in
          Some (merge' ?hook ~witness:(Entry.v key value) t)
    else None

  let replace ?overcommit t key value =
    ignore (replace' ?hook:None ?overcommit t key value : _ async option)

  let replace_with_timer ?sampling_interval t key value =
    if sampling_interval <> None then Stats.start_replace ();
    replace t key value;
    match sampling_interval with
    | None -> ()
    | Some sampling_interval -> Stats.end_replace ~sampling_interval

  let filter t f =
    let t = check_open t in
    Log.debug (fun l -> l "[%s] filter" (Filename.basename t.root));
    if t.config.readonly then raise RO_not_allowed;
    let witness =
      Semaphore.with_acquire "witness" t.rename_lock (fun () -> get_witness t)
    in
    match witness with
    | None ->
        Log.debug (fun l -> l "[%s] index is empty" (Filename.basename t.root))
    | Some witness -> (
        match Thread.await (merge' ~blocking:true ~filter:f ~witness t) with
        | Ok (`Aborted | `Completed) -> ()
        | Error (`Async_exn exn) ->
            Fmt.failwith "filter: asynchronous exception during merge (%s)"
              (Printexc.to_string exn))

  let iter f t =
    let t = check_open t in
    Log.debug (fun l -> l "[%s] iter" (Filename.basename t.root));
    match t.log with
    | None -> ()
    | Some log ->
        Tbl.iter f log.mem;
        may (fun (i : index) -> iter_io (fun e -> f e.key e.value) i.io) t.index;
        Semaphore.with_acquire "iter" t.rename_lock (fun () ->
            match t.log_async with None -> () | Some log -> Tbl.iter f log.mem)

  let close' ~hook ?immediately it =
    let abort_merge =
      match immediately with Some () -> true | None -> false
    in
    match !it with
    | None -> Log.debug (fun l -> l "close: instance already closed")
    | Some t ->
        Log.debug (fun l -> l "[%s] close" (Filename.basename t.root));
        if abort_merge then (
          t.pending_cancel <- true;
          hook `Abort_signalled);
        Semaphore.with_acquire "close" t.merge_lock (fun () ->
            it := None;
            t.open_instances <- t.open_instances - 1;
            if t.open_instances = 0 then (
              Log.debug (fun l ->
                  l "[%s] last open instance: closing the file descriptor"
                    (Filename.basename t.root));
              if not t.config.readonly then flush_instance ~with_fsync:true t;
              may
                (fun l ->
                  Tbl.clear l.mem;
                  IO.close l.io)
                t.log;
              may (fun (i : index) -> IO.close i.io) t.index;
              may (fun lock -> IO.Lock.unlock lock) t.writer_lock))

  let close = close' ~hook:(fun _ -> ())

  let sync' ?hook t =
    let f t =
      Stats.incr_nb_sync ();
      let t = check_open t in
      Log.debug (fun l -> l "[%s] sync" (Filename.basename t.root));
      if t.config.readonly then sync_log ?hook t else raise RW_not_allowed
    in
    Stats.sync_with_timer (fun () -> f t)

  let sync = sync' ?hook:None

  module Checks = Checks.Make (K) (V) (IO)
end

module Make = Make_private

module Private = struct
  module Fan = Fan
  module Io = Io
  module Io_array = Io_array
  module Search = Search
  module Data = Data

  module Hook = struct
    type 'a t = 'a -> unit

    let v f = f
  end

  module type S = Private with type 'a hook := 'a Hook.t

  module Make = Make_private
end
