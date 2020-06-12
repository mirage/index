external pread : Unix.file_descr -> int64 -> bytes -> int -> int -> int
  = "caml_pread"

let pread ~fd ~fd_offset ~buffer ~buffer_offset ~length =
  pread fd fd_offset buffer buffer_offset length

external pwrite : Unix.file_descr -> int64 -> bytes -> int -> int -> int
  = "caml_pwrite"

let pwrite ~fd ~fd_offset ~buffer ~buffer_offset ~length =
  pwrite fd fd_offset buffer buffer_offset length

external fsync : Unix.file_descr -> unit = "unix_fsync"
