val pread :
  fd:Unix.file_descr ->
  fd_offset:int64 ->
  buffer:bytes ->
  buffer_offset:int ->
  length:int ->
  int
(** Reads up to [length] bytes from [fd] (starting at position [fd_offset]) into
    [buffer] (starting at position [buffer_offset]). Returns the number of bytes
    actually read. [fd]'s cursor position is unchanged. *)

val pwrite :
  fd:Unix.file_descr ->
  fd_offset:int64 ->
  buffer:bytes ->
  buffer_offset:int ->
  length:int ->
  int
(** Writes up to [length] bytes from [buffer] (starting at position
    [buffer_offset]) to the file descriptor [fd] (starting at position
    [fd_offset]). Returns the number of bytes actually written. [fd]'s cursor
    position is unchanged. *)

val fsync : Unix.file_descr -> unit
(** Flushes all modified buffer cache pages for a given file descriptor to the
    disk device, so that all changed information can be retrieved even if the
    system crashes or is rebooted. This includes writing through or flushing a
    disk cache if present. Blocks until the device reports that the transfer has
    completed. *)
