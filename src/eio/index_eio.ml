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

open! Import

let src = Logs.Src.create "index_eio" ~doc:"Index_eio"

module Log = (val Logs.src_log src : Logs.LOG)

exception RO_not_allowed

let current_version = "00000001"

module Stats = Index.Stats

type io = { switch : Eio.Switch.t; root : Eio.Fs.dir_ty Eio.Path.t }

module IO : Index.Platform.IO with type io = io = struct
  type nonrec io = io

  let ( ++ ) = Int63.add
  let ( -- ) = Int63.sub

  type t = {
    mutable file : Eio.Fs.dir_ty Eio.Path.t;
    mutable header : int63;
    mutable raw : Raw.t;
    mutable offset : int63;
    mutable flushed : int63;
    mutable fan_size : int63;
    readonly : bool;
    buf : Buffer.t;
    flush_callback : unit -> unit;
    sw : Eio.Switch.t;
  }

  let flush ?no_callback ?(with_fsync = false) t =
    if t.readonly then raise RO_not_allowed;
    if not (Buffer.is_empty t.buf) then (
      let buf_len = Buffer.length t.buf in
      let offset = t.offset in
      (match no_callback with Some () -> () | None -> t.flush_callback ());
      Log.debug (fun l -> l "[%a] flushing %d bytes" Eio.Path.pp t.file buf_len);
      Buffer.write_with (Raw.unsafe_write t.raw ~off:t.flushed) t.buf;
      Buffer.clear t.buf;
      Raw.Offset.set t.raw offset;
      assert (t.flushed ++ Int63.of_int buf_len = t.header ++ offset);
      t.flushed <- offset ++ t.header);
    if with_fsync then Raw.fsync t.raw

  let rename ~src ~dst =
    flush ~with_fsync:true src;
    Raw.close dst.raw;
    Eio.Path.rename src.file dst.file;
    Buffer.clear dst.buf;
    src.file <- dst.file;
    dst.header <- src.header;
    dst.fan_size <- src.fan_size;
    dst.offset <- src.offset;
    dst.flushed <- src.flushed;
    dst.raw <- src.raw

  let close t =
    if not t.readonly then Buffer.clear t.buf;
    Raw.close t.raw

  let auto_flush_limit = Int63.of_int 1_000_000

  let append_substring t buf ~off ~len =
    if t.readonly then raise RO_not_allowed;
    Buffer.add_substring t.buf buf ~off ~len;
    let len = Int63.of_int len in
    t.offset <- t.offset ++ len;
    if t.offset -- t.flushed > auto_flush_limit then flush t

  let append t buf = append_substring t buf ~off:0 ~len:(String.length buf)

  let read t ~off ~len buf =
    let off = t.header ++ off in
    let end_of_value = off ++ Int63.of_int len in
    if not t.readonly then
      assert (
        let total_length = t.flushed ++ Int63.of_int (Buffer.length t.buf) in
        (* NOTE: we don't require that [end_of_value <= total_length] in order
           to support short reads on read-write handles (see comment about this
           case below). *)
        off <= total_length);

    if t.readonly || end_of_value <= t.flushed then
      (* Value is entirely on disk *)
      Raw.unsafe_read t.raw ~off ~len buf
    else
      (* Must read some data not yet flushed to disk *)
      let requested_from_disk = max 0 (Int63.to_int (t.flushed -- off)) in
      let requested_from_buffer = len - requested_from_disk in
      let read_from_disk =
        if requested_from_disk > 0 then (
          let read = Raw.unsafe_read t.raw ~off ~len:requested_from_disk buf in
          assert (read = requested_from_disk);
          read)
        else 0
      in
      let read_from_buffer =
        let src_off = max 0 (Int63.to_int (off -- t.flushed)) in
        let len =
          (* The user may request more bytes than actually exist, in which case
             we read to the end of the write buffer and return a size less than
             [len]. *)
          let available_length = Buffer.length t.buf - src_off in
          min available_length requested_from_buffer
        in
        Buffer.blit ~src:t.buf ~src_off ~dst:buf ~dst_off:requested_from_disk
          ~len;
        len
      in
      read_from_disk + read_from_buffer

  let offset t = t.offset

  let get_generation t =
    let i = Raw.Generation.get t.raw in
    Log.debug (fun m -> m "get_generation: %a" Int63.pp i);
    i

  let get_fanout t = Raw.Fan.get t.raw
  let get_fanout_size t = Raw.Fan.get_size t.raw

  let set_fanout t buf =
    assert (Int63.(equal (of_int (String.length buf)) t.fan_size));
    Raw.Fan.set t.raw buf

  module Header = struct
    type header = { offset : int63; generation : int63 }

    let pp ppf { offset; generation } =
      Format.fprintf ppf "{ offset = %a; generation = %a }" Int63.pp offset
        Int63.pp generation

    let get t =
      let Raw.Header.{ offset; generation; _ } = Raw.Header.get t.raw in
      t.offset <- offset;
      let headers = { offset; generation } in
      Log.debug (fun m ->
          m "[%a] get_headers: %a" Eio.Path.pp t.file pp headers);
      headers

    let set t { offset; generation } =
      let version = current_version in
      Log.debug (fun m ->
          m "[%a] set_header %a" Eio.Path.pp t.file pp { offset; generation });
      Raw.Header.(set t.raw { offset; version; generation })
  end

  let mkdir dirname = Eio.Path.mkdirs ~exists_ok:true ~perm:0o755 dirname

  let mkdir_of file =
    match Eio.Path.split file with
    | None -> ()
    | Some (dirname, _) -> mkdir dirname

  let raw_file ~sw ~version ~offset ~generation file =
    Eio.Switch.run @@ fun _sw ->
    let x = Eio.Path.open_out ~sw ~create:(`If_missing 0o644) file in
    let raw = Raw.v x in
    let header = { Raw.Header.offset; version; generation } in
    Log.debug (fun m ->
        m "[%a] raw set_header %a" Eio.Path.pp file Header.pp
          { offset; generation });
    Raw.Header.set raw header;
    Raw.Fan.set raw "";
    Raw.fsync raw;
    raw

  let clear ~generation ?(hook = fun () -> ()) ~reopen t =
    t.offset <- Int63.zero;
    t.flushed <- t.header;
    Buffer.clear t.buf;
    let old = t.raw in

    if reopen then (
      (* Open a fresh file and rename it to ensure atomicity:
         concurrent readers should never see the file disapearing. *)
      let tmp_file =
        let dir, path = t.file in
        (dir, path ^ "_tmp")
      in
      t.raw <-
        raw_file ~sw:t.sw ~version:current_version ~generation
          ~offset:Int63.zero tmp_file;
      Eio.Path.rename tmp_file t.file)
    else
      (* Remove the file current file. This allows a fresh file to be
         created, before writing the new generation in the old file. *)
      Eio.Path.unlink t.file;

    hook ();

    (* Set new generation in the old file. *)
    Raw.Header.set old
      { Raw.Header.offset = Int63.zero; generation; version = current_version };
    Raw.close old

  let () = assert (String.length current_version = 8)

  let v_instance ~sw ?(flush_callback = fun () -> ()) ~readonly ~fan_size
      ~offset file raw =
    let eight = Int63.of_int 8 in
    let header = eight ++ eight ++ eight ++ eight ++ fan_size in
    {
      header;
      file;
      offset;
      raw;
      readonly;
      fan_size;
      buf = Buffer.create (if readonly then 0 else 4 * 1024);
      flushed = header ++ offset;
      flush_callback;
      sw;
    }

  let v ~io ?flush_callback ~fresh ~generation ~fan_size filename =
    let file = Eio.Path.(io.root / filename) in
    let v = v_instance ~sw:io.switch ?flush_callback ~readonly:false file in
    mkdir_of file;
    let header =
      { Raw.Header.offset = Int63.zero; version = current_version; generation }
    in
    match Eio.Path.is_file file with
    | false ->
        let x =
          Eio.Path.open_out ~sw:io.switch file ~create:(`Exclusive 0o644)
        in
        let raw = Raw.v x in
        Raw.Header.set raw header;
        Raw.Fan.set_size raw fan_size;
        v ~fan_size ~offset:Int63.zero raw
    | true ->
        let x = Eio.Path.open_out ~sw:io.switch file ~create:`Never in
        let raw = Raw.v x in
        if fresh then (
          Raw.Header.set raw header;
          Raw.Fan.set_size raw fan_size;
          Raw.fsync raw;
          v ~fan_size ~offset:Int63.zero raw)
        else
          let version = Raw.Version.get raw in
          if version <> current_version then
            Fmt.failwith "Io.v: unsupported version %s (current version is %s)"
              version current_version;

          let offset = Raw.Offset.get raw in
          let fan_size = Raw.Fan.get_size raw in
          v ~fan_size ~offset raw

  let v_readonly ~io filename =
    let file = Eio.Path.(io.root / filename) in
    let v = v_instance ~sw:io.switch ~readonly:true file in
    try
      let x = Eio.Path.open_out ~sw:io.switch ~create:`Never file in
      let raw = Raw.v x in
      try
        let version = Raw.Version.get raw in
        if version <> current_version then
          Fmt.failwith "Io.v: unsupported version %s (current version is %s)"
            version current_version;
        let offset = Raw.Offset.get raw in
        let fan_size = Raw.Fan.get_size raw in
        Ok (v ~fan_size ~offset raw)
      with Raw.Not_written ->
        (* The readonly instance cannot read a file that does not have a
           header.*)
        Raw.close raw;
        Error `No_file_on_disk
    with
    | Eio.Io (Eio.Fs.E (Not_found _), _) -> Error `No_file_on_disk
    | e -> raise e

  let exists = Sys.file_exists
  let size { raw; _ } = (Raw.fstat raw).Eio.File.Stat.size |> Int63.to_int
  let size_header t = t.header |> Int63.to_int

  module Lock = struct
    type t = { path : string; fd : Eio.File.rw_ty Eio.Resource.t }

    exception Locked of string

    let single_write fd str =
      let cstruct = Cstruct.string str in
      Eio.File.pwrite_single fd ~file_offset:Int63.zero [ cstruct ]

    let unsafe_lock ~io _op filename =
      let f = Eio.Path.(io.root / filename) in
      mkdir_of f;
      let fd = Eio.Path.open_out ~sw:io.switch ~create:(`If_missing 0o600) f in
      let pid = string_of_int (Unix.getpid ()) in
      let pid_len = String.length pid in
      try
        (* TODO: Unix.lockf fd op 0; *)
        if single_write fd pid <> pid_len then (
          Eio.Resource.close fd;
          failwith "Unable to write PID to lock file")
        else Some fd
      with
      (* TODO:
         | Unix.Unix_error (Unix.EAGAIN, _, _) ->
             Eio.Resource.close fd;
             None *)
      | e ->
        Eio.Resource.close fd;
        raise e

    let with_ic path f =
      let ic = open_in path in
      let a = f ic in
      close_in ic;
      a

    let err_rw_lock path =
      let line = with_ic path input_line in
      let pid = int_of_string line in
      Log.err (fun l ->
          l
            "Cannot lock %s: index is already opened in write mode by PID %d. \
             Current PID is %d."
            path pid (Unix.getpid ()));
      raise (Locked path)

    let lock ~io path =
      Log.debug (fun l -> l "Locking %s" path);
      match unsafe_lock ~io Unix.F_TLOCK path with
      | Some fd -> { path; fd }
      | None -> err_rw_lock path

    let unlock { path; fd } =
      Log.debug (fun l -> l "Unlocking %s" path);
      Eio.Resource.close fd

    let pp_dump path =
      match Sys.file_exists path with
      | false -> None
      | true ->
          let contents =
            with_ic path (fun ic ->
                really_input_string ic (in_channel_length ic))
          in
          Some (fun ppf -> Fmt.string ppf contents)
  end
end

module Semaphore = struct
  module S = Eio.Mutex

  type t = S.t

  let is_held t =
    let acquired = S.try_lock t in
    if acquired then S.unlock t;
    not acquired

  let make b =
    let t = S.create () in
    if not b then S.lock t;
    t

  let release t = S.unlock t

  let acquire n t =
    let x = Mtime_clock.counter () in
    S.lock t;
    let y = Mtime_clock.count x in
    if Mtime.span_to_s y > 1. then
      Log.warn (fun l -> l "Semaphore %s was blocked for %a" n Mtime.Span.pp y)

  let with_acquire n t f =
    acquire n t;
    Fun.protect ~finally:(fun () -> S.unlock t) f
end

module Thread = struct
  type io = IO.io
  type 'a t = 'a Eio.Promise.or_exn

  let async ~io f = Eio.Fiber.fork_promise ~sw:io.switch f
  let yield = Eio.Fiber.yield
  let return a = Eio.Promise.create_resolved (Ok a)

  let await t =
    match Eio.Promise.await t with
    | Ok v -> Ok v
    | Error exn -> Error (`Async_exn exn)
end

module Platform = struct
  type nonrec io = io

  module IO = IO
  module Semaphore = Semaphore
  module Thread = Thread
  module Clock = Mtime_clock
  module Progress = Progress
  module Fmt_tty = Fmt_tty
end

module Make (K : Index.Key.S) (V : Index.Value.S) (C : Index.Cache.S) =
  Index.Make (K) (V) (Platform) (C)

module Private = struct
  module Platform = Platform
  module IO = IO
  module Raw = Raw

  module Make (K : Index.Key.S) (V : Index.Value.S) =
    Index.Private.Make (K) (V) (Platform)
end
