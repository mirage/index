external pread_int : Unix.file_descr -> int -> bytes -> int -> int -> int
  = "caml_index_pread_int"

external pread_int64 : Unix.file_descr -> int64 -> bytes -> int -> int -> int
  = "caml_index_pread_int64"

let pread : fd:_ -> fd_offset:Int63.t -> _ =
  match Int63.observe with
  | Int Refl ->
      fun ~fd ~fd_offset ~buffer ~buffer_offset ~length ->
        pread_int fd fd_offset buffer buffer_offset length
  | Int64 cast ->
      fun ~fd ~fd_offset ~buffer ~buffer_offset ~length ->
        pread_int64 fd (cast fd_offset) buffer buffer_offset length
  | _ -> assert false

external pwrite_int : Unix.file_descr -> int -> bytes -> int -> int -> int
  = "caml_index_pwrite_int"

external pwrite_int64 : Unix.file_descr -> int64 -> bytes -> int -> int -> int
  = "caml_index_pwrite_int64"

let pwrite : fd:_ -> fd_offset:Int63.t -> _ =
  match Int63.observe with
  | Int Refl ->
      fun ~fd ~fd_offset ~buffer ~buffer_offset ~length ->
        pwrite_int fd fd_offset buffer buffer_offset length
  | Int64 cast ->
      fun ~fd ~fd_offset ~buffer ~buffer_offset ~length ->
        pwrite_int64 fd (cast fd_offset) buffer buffer_offset length
  | _ -> assert false
