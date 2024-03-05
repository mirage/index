open! Import
module Stats = Index.Stats

let ( ++ ) = Int63.add

type t = Rw : _ Eio.File.rw -> t [@@unboxed]

let v fd = Rw fd

let really_write fd fd_offset buffer buffer_offset length =
  let cstruct = Cstruct.string ~off:buffer_offset ~len:length buffer in
  Eio.File.pwrite_all fd ~file_offset:fd_offset [ cstruct ]
(*
  let rec aux fd_offset buffer_offset length =
    let w = Syscalls.pwrite ~fd ~fd_offset ~buffer ~buffer_offset ~length in
    if w = 0 || w = length then ()
    else
      (aux [@tailcall])
        (fd_offset ++ Int63.of_int w)
        (buffer_offset + w) (length - w)
  in
  aux fd_offset buffer_offset length
  *)

let really_read fd fd_offset length buffer =
  let cstruct = Cstruct.create length in
  Eio.File.pread_exact fd ~file_offset:fd_offset [ cstruct ];
  Cstruct.blit_to_bytes cstruct 0 buffer 0 length;
  length
(*
  let rec aux fd_offset buffer_offset length =
    let r = Syscalls.pread ~fd ~fd_offset ~buffer ~buffer_offset ~length in
    if r = 0 then buffer_offset (* end of file *)
    else if r = length then buffer_offset + r
    else
      (aux [@tailcall])
        (fd_offset ++ Int63.of_int r)
        (buffer_offset + r) (length - r)
  in
  aux fd_offset 0 length
  *)

let fsync (Rw t) = Eio.File.sync t
let close (Rw t) = Eio.Resource.close t
let fstat (Rw t) = Eio.File.stat t

let unsafe_write (Rw t) ~off buffer buffer_offset length =
  really_write t off buffer buffer_offset length;
  Stats.add_write length

let unsafe_read (Rw t) ~off ~len buf =
  match really_read t off len buf with
  | n ->
      Stats.add_read n;
      n
  | exception End_of_file -> 0

let encode_int63 n =
  let buf = Bytes.create Int63.encoded_size in
  Int63.encode buf ~off:0 n;
  Bytes.unsafe_to_string buf

let decode_int63 buf = Int63.decode ~off:0 buf

exception Not_written

let assert_read ~len n =
  if n = 0 && n <> len then raise Not_written;
  assert (
    if Int.equal n len then true
    else (
      Printf.eprintf "Attempted to read %d bytes, but got %d bytes instead!\n%!"
        len n;
      false))
[@@inline always]

module Offset = struct
  let off = Int63.zero
  let set t n = unsafe_write t ~off (encode_int63 n) 0 8

  let get t =
    let len = 8 in
    let buf = Bytes.create len in
    let n = unsafe_read t ~off ~len buf in
    assert_read ~len n;
    decode_int63 (Bytes.unsafe_to_string buf)
end

module Version = struct
  let off = Int63.of_int 8

  let get t =
    let len = 8 in
    let buf = Bytes.create len in
    let n = unsafe_read t ~off ~len buf in
    assert_read ~len n;
    Bytes.unsafe_to_string buf

  let set t v = unsafe_write t ~off v 0 8
end

module Generation = struct
  let off = Int63.of_int 16

  let get t =
    let len = 8 in
    let buf = Bytes.create len in
    let n = unsafe_read t ~off ~len buf in
    assert_read ~len n;
    decode_int63 (Bytes.unsafe_to_string buf)

  let set t gen = unsafe_write t ~off (encode_int63 gen) 0 8
end

module Fan = struct
  let off = Int63.of_int 24

  let set t buf =
    let buf_len = String.length buf in
    let size = encode_int63 (Int63.of_int buf_len) in
    unsafe_write t ~off size 0 8;
    if buf <> "" then unsafe_write t ~off:(off ++ Int63.of_int 8) buf 0 buf_len

  let get_size t =
    let len = 8 in
    let size_buf = Bytes.create len in
    let n = unsafe_read t ~off ~len size_buf in
    assert_read ~len n;
    decode_int63 (Bytes.unsafe_to_string size_buf)

  let set_size t size =
    let buf = encode_int63 size in
    unsafe_write t ~off buf 0 8

  let get t =
    let size = Int63.to_int (get_size t) in
    let buf = Bytes.create size in
    let n = unsafe_read t ~off:(off ++ Int63.of_int 8) ~len:size buf in
    assert_read ~len:size n;
    Bytes.unsafe_to_string buf
end

module Header = struct
  type t = { offset : int63; version : string; generation : int63 }

  (** NOTE: These functions must be equivalent to calling the above [set] /
      [get] functions individually. *)

  let total_header_length = 8 + 8 + 8

  let read_word buf off =
    let result = Bytes.create 8 in
    Bytes.blit buf off result 0 8;
    Bytes.unsafe_to_string result

  let get t =
    let header = Bytes.create total_header_length in
    let n = unsafe_read t ~off:Int63.zero ~len:total_header_length header in
    assert_read ~len:total_header_length n;
    let offset = read_word header 0 |> decode_int63 in
    let version = read_word header 8 in
    let generation = read_word header 16 |> decode_int63 in
    { offset; version; generation }

  let set t { offset; version; generation } =
    assert (String.length version = 8);
    let b = Bytes.create total_header_length in
    Bytes.blit_string (encode_int63 offset) 0 b 0 8;
    Bytes.blit_string version 0 b 8 8;
    Bytes.blit_string (encode_int63 generation) 0 b 16 8;
    unsafe_write t ~off:Int63.zero (Bytes.unsafe_to_string b) 0
      total_header_length
end

module Header_prefix = struct
  type t = { offset : int63; version : string }

  (** NOTE: These functions must be equivalent to calling the above [set] /
      [get] functions individually. *)

  let total_header_length = 8 + 8

  let read_word buf off =
    let result = Bytes.create 8 in
    Bytes.blit buf off result 0 8;
    Bytes.unsafe_to_string result

  let get t =
    let header = Bytes.create total_header_length in
    let n = unsafe_read t ~off:Int63.zero ~len:total_header_length header in
    assert_read ~len:total_header_length n;
    let offset = read_word header 0 |> decode_int63 in
    let version = read_word header 8 in
    { offset; version }

  let set t { offset; version } =
    assert (String.length version = 8);
    let b = Bytes.create total_header_length in
    Bytes.blit_string (encode_int63 offset) 0 b 0 8;
    Bytes.blit_string version 0 b 8 8;
    unsafe_write t ~off:Int63.zero (Bytes.unsafe_to_string b) 0
      total_header_length
end
