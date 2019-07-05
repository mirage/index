module IO : Index.IO = struct
  let ( ++ ) = Int64.add

  let ( -- ) = Int64.sub

  external set_64 : Bytes.t -> int -> int64 -> unit = "%caml_string_set64u"

  external get_64 : string -> int -> int64 = "%caml_string_get64"

  external swap64 : int64 -> int64 = "%bswap_int64"

  let encode_int64 i =
    let set_uint64 s off v =
      if not Sys.big_endian then set_64 s off (swap64 v) else set_64 s off v
    in
    let b = Bytes.create 8 in
    set_uint64 b 0 i;
    Bytes.to_string b

  let decode_int64 buf =
    let get_uint64 s off =
      if not Sys.big_endian then swap64 (get_64 s off) else get_64 s off
    in
    get_uint64 buf 0

  module Raw = struct
    type t = { fd : Unix.file_descr; mutable cursor : int64 }

    let v fd = { fd; cursor = 0L }

    let really_write fd buf =
      let rec aux off len =
        let w = Unix.write fd buf off len in
        if w = 0 then () else (aux [@tailcall]) (off + w) (len - w)
      in
      (aux [@tailcall]) 0 (Bytes.length buf)

    let really_read fd buf =
      let rec aux off len =
        let r = Unix.read fd buf off len in
        if r = 0 || r = len then off + r
        else (aux [@tailcall]) (off + r) (len - r)
      in
      (aux [@tailcall]) 0 (Bytes.length buf)

    let lseek t off =
      if off = t.cursor then ()
      else
        let _ = Unix.LargeFile.lseek t.fd off Unix.SEEK_SET in
        t.cursor <- off

    let unsafe_write t ~off buf =
      lseek t off;
      let buf = Bytes.unsafe_of_string buf in
      really_write t.fd buf;
      t.cursor <- off ++ Int64.of_int (Bytes.length buf)

    let unsafe_read t ~off buf =
      lseek t off;
      let n = really_read t.fd buf in
      t.cursor <- off ++ Int64.of_int n;
      n

    let unsafe_set_offset t n =
      let buf = encode_int64 n in
      unsafe_write t ~off:0L buf

    let unsafe_get_offset t =
      let buf = Bytes.create 8 in
      let n = unsafe_read t ~off:0L buf in
      assert (n = 8);
      decode_int64 (Bytes.unsafe_to_string buf)
  end

  type t = {
    file : string;
    mutable raw : Raw.t;
    mutable offset : int64;
    mutable flushed : int64;
    buf : Buffer.t;
  }

  let header = 8L

  let flush t =
    let buf = Buffer.contents t.buf in
    let offset = t.offset in
    Buffer.clear t.buf;
    if buf = "" then ()
    else (
      Raw.unsafe_write t.raw ~off:t.flushed buf;
      Raw.unsafe_set_offset t.raw offset;
      if not (t.flushed ++ Int64.of_int (String.length buf) = header ++ offset)
      then
        failwith
          (Printf.sprintf "sync error: %s flushed=%Ld offset+header=%Ld\n%!"
             t.file t.flushed (offset ++ header));
      t.flushed <- offset ++ header )

  let rename ~src ~dst =
    flush src;
    Unix.close dst.raw.fd;
    Unix.rename src.file dst.file;
    dst.offset <- src.offset;
    dst.flushed <- src.flushed;
    dst.raw <- src.raw

  let auto_flush_limit = 1_000_000L

  let append t buf =
    Buffer.add_string t.buf buf;
    let len = Int64.of_int (String.length buf) in
    t.offset <- t.offset ++ len;
    if t.offset -- t.flushed > auto_flush_limit then flush t

  let read t ~off buf =
    assert (header ++ off <= t.flushed);
    Raw.unsafe_read t.raw ~off:(header ++ off) buf

  let offset t = t.offset

  let name t = t.file

  let protect_unix_exn = function
    | Unix.Unix_error _ as e -> failwith (Printexc.to_string e)
    | e -> raise e

  let ignore_enoent = function
    | Unix.Unix_error (Unix.ENOENT, _, _) -> ()
    | e -> raise e

  let protect f x = try f x with e -> protect_unix_exn e

  let safe f x = try f x with e -> ignore_enoent e

  let mkdir dirname =
    let rec aux dir k =
      if Sys.file_exists dir && Sys.is_directory dir then k ()
      else (
        if Sys.file_exists dir then safe Unix.unlink dir;
        (aux [@tailcall]) (Filename.dirname dir) @@ fun () ->
        protect (Unix.mkdir dir) 0o755;
        k () )
    in
    (aux [@tailcall]) dirname (fun () -> ())

  let clear t =
    t.offset <- 0L;
    t.flushed <- header;
    Buffer.clear t.buf

  let buffers = Hashtbl.create 256

  let buffer file =
    try
      let buf = Hashtbl.find buffers file in
      Buffer.clear buf;
      buf
    with Not_found ->
      let buf = Buffer.create (4 * 1024) in
      Hashtbl.add buffers file buf;
      buf

  let v ?(read_only = false) file =
    let v ~offset raw =
      { file; offset; raw; buf = buffer file; flushed = header ++ offset }
    in
    let mode = Unix.(if read_only then O_RDONLY else O_RDWR) in
    mkdir (Filename.dirname file);
    match Sys.file_exists file with
    | false ->
        if read_only then raise Index.RO_Not_Allowed;
        let x = Unix.openfile file Unix.[ O_CREAT; mode ] 0o644 in
        let raw = Raw.v x in
        Raw.unsafe_set_offset raw 0L;
        v ~offset:0L raw
    | true ->
        let x = Unix.openfile file Unix.[ O_EXCL; mode ] 0o644 in
        let raw = Raw.v x in
        let offset = Raw.unsafe_get_offset raw in
        v ~offset raw
end

module Make (K : Index.Key) (V : Index.Value) = Index.Make (K) (V) (IO)
