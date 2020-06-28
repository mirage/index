let ( ++ ) = Int64.add

module Stats = Index.Stats

external set_64 : Bytes.t -> int -> int64 -> unit = "%caml_string_set64u"

external get_64 : string -> int -> int64 = "%caml_string_get64"

external swap64 : int64 -> int64 = "%bswap_int64"

let encode_int64 i =
  let set_uint64 s off v =
    if not Sys.big_endian then set_64 s off (swap64 v) else set_64 s off v
  in
  let b = Bytes.create 8 in
  set_uint64 b 0 i;
  Bytes.unsafe_to_string b

let decode_int64 buf =
  let get_uint64 s off =
    if not Sys.big_endian then swap64 (get_64 s off) else get_64 s off
  in
  get_uint64 buf 0

type t = { fd : Unix.file_descr } [@@unboxed]

type raw = t

let v fd = { fd }

let really_write fd fd_offset buffer =
  let rec aux fd_offset buffer_offset length =
    let w = Syscalls.pwrite ~fd ~fd_offset ~buffer ~buffer_offset ~length in
    if w = 0 || w = length then ()
    else
      (aux [@tailcall])
        (fd_offset ++ Int64.of_int w)
        (buffer_offset + w) (length - w)
  in
  (aux [@tailcall]) fd_offset 0 (Bytes.length buffer)

let really_read fd fd_offset length buffer =
  let rec aux fd_offset buffer_offset length =
    let r = Syscalls.pread ~fd ~fd_offset ~buffer ~buffer_offset ~length in
    if r = 0 then buffer_offset (* end of file *)
    else if r = length then buffer_offset + r
    else
      (aux [@tailcall])
        (fd_offset ++ Int64.of_int r)
        (buffer_offset + r) (length - r)
  in
  (aux [@tailcall]) fd_offset 0 length

let fsync t = Syscalls.fsync t.fd

let close t = Unix.close t.fd

let unsafe_write t ~off buf =
  let buf = Bytes.unsafe_of_string buf in
  really_write t.fd off buf;
  Stats.add_write (Bytes.length buf)

let unsafe_read t ~off ~len buf =
  let n = really_read t.fd off len buf in
  Stats.add_read n;
  n

module Offset = struct
  let set t n =
    let buf = encode_int64 n in
    unsafe_write t ~off:0L buf

  let get t =
    let buf = Bytes.create 8 in
    let n = unsafe_read t ~off:0L ~len:8 buf in
    assert (n = 8);
    decode_int64 (Bytes.unsafe_to_string buf)
end

module Version = struct
  let get t =
    let buf = Bytes.create 8 in
    let n = unsafe_read t ~off:8L ~len:8 buf in
    assert (n = 8);
    Bytes.unsafe_to_string buf

  let set t v = unsafe_write t ~off:8L v
end

module Generation = struct
  let get t =
    let buf = Bytes.create 8 in
    let n = unsafe_read t ~off:16L ~len:8 buf in
    assert (n = 8);
    decode_int64 (Bytes.unsafe_to_string buf)

  let set t gen =
    let buf = encode_int64 gen in
    unsafe_write t ~off:16L buf
end

module Fan = struct
  let set t buf =
    let size = encode_int64 (Int64.of_int (String.length buf)) in
    unsafe_write t ~off:24L size;
    if buf <> "" then unsafe_write t ~off:(24L ++ 8L) buf

  let get_size t =
    let size_buf = Bytes.create 8 in
    let n = unsafe_read t ~off:24L ~len:8 size_buf in
    assert (n = 8);
    decode_int64 (Bytes.unsafe_to_string size_buf)

  let set_size t size =
    let buf = encode_int64 size in
    unsafe_write t ~off:24L buf

  let get t =
    let size = Int64.to_int (get_size t) in
    let buf = Bytes.create size in
    let n = unsafe_read t ~off:(24L ++ 8L) ~len:size buf in
    assert (n = size);
    Bytes.unsafe_to_string buf
end

module Header = struct
  type t = { offset : int64; version : string; generation : int64 }

  (** NOTE: These functions must be equivalent to calling the above [set] /
      [get] functions individually. *)

  let total_header_length = 8 + 8 + 8

  let read_word buf off =
    let result = Bytes.create 8 in
    Bytes.blit buf off result 0 8;
    Bytes.unsafe_to_string result

  let get t =
    let header = Bytes.create total_header_length in
    let n = unsafe_read t ~off:0L ~len:total_header_length header in
    assert (n = total_header_length);
    let offset = read_word header 0 |> decode_int64 in
    let version = read_word header 8 in
    let generation = read_word header 16 |> decode_int64 in
    { offset; version; generation }

  let set t { offset; version; generation } =
    assert (String.length version = 8);
    let b = Bytes.create total_header_length in
    Bytes.blit_string (encode_int64 offset) 0 b 0 8;
    Bytes.blit_string version 0 b 8 8;
    Bytes.blit_string (encode_int64 generation) 0 b 16 8;
    unsafe_write t ~off:0L (Bytes.unsafe_to_string b)
end
