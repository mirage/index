(** [Raw] wraps a file descriptor with the file format used by Index.

    The file starts with a header made of four fields, followed by the data:

    - {b offset} (bytes 0 to 7): the length of the valid data that follows the
      header. This module does not interpret it: callers update it after writing
      data, and {!fsync} does not write it;
    - {b version} (bytes 8 to 15): an 8-byte string, not interpreted by this
      module. Index writes ["00000001"] and refuses to open a file with another
      version;
    - {b generation} (bytes 16 to 23): not interpreted by this module. Index
      increments it on every [clear] and [merge], and writes it in the headers
      of the files it rewrites. Read-only instances compare the generation of
      the log file with the one they know to decide whether to reload their
      files;
    - {b fan} (from byte 24): the length of the fan on bytes 24 to 31, followed
      by the fan itself. Index stores the fan-out of its data file there, and an
      empty fan in its log files.

    Offset, generation and the length of the fan are [int63] values encoded on 8
    bytes, in big-endian order.

    Because of the fan, the header has no fixed size: it is 32 bytes plus the
    length of the fan. This module does not compute it: callers do, as
    [Index_unix] does.

    Files written with {!Header_prefix} have a different layout: their header is
    made of the offset and the version only, and their data starts at byte 16.

    The functions below that read the header raise {!Not_written} when the file
    ends before the last byte they read. *)

open! Import

type t
(** The type of [raw] file handles. *)

val v : Unix.file_descr -> t
(** [v fd] is a [raw] file handle for [fd]. It does not read or write anything:
    the caller must initialise the header of a new file. *)

val unsafe_write : t -> off:int63 -> string -> int -> int -> unit
(** [unsafe_write t ~off s soff len] writes the [len] bytes of [s] starting at
    [soff] to the file, at offset [off]. Offsets are absolute: the caller must
    write past the header.

    The bounds are not checked: [soff] and [len] must designate a valid
    substring of [s].

    @raise Unix.Unix_error if a system call fails. *)

val unsafe_read : t -> off:int63 -> len:int -> bytes -> int
(** [unsafe_read t ~off ~len buf] reads at most [len] bytes from the file, at
    offset [off], into [buf] starting at index [0]. It returns the number of
    bytes read, which is less than [len] only at the end of the file. Offsets
    are absolute: the caller must read past the header.

    The bounds are not checked: [len] must be between [0] and the length of
    [buf].

    @raise Unix.Unix_error if a system call fails. *)

val fsync : t -> unit
(** [fsync t] flushes the file to disk with [Unix.fsync]. *)

val close : t -> unit
(** [close t] closes the underlying file descriptor. *)

val fstat : t -> Unix.stats
(** [fstat t] is the result of [Unix.fstat] on the underlying file descriptor.
    Its [st_size] includes the header. *)

exception Not_written
(** The exception raised when reading the header of a file that ends before the
    last byte to read: the header is missing, as in an empty file, or was only
    partly written. *)

module Version : sig
  val get : t -> string
  (** [get t] is the version, a string of 8 bytes. *)

  val set : t -> string -> unit
  (** [set t s] writes [s] as the version.

      @raise Invalid_argument if [s] is not 8 bytes long. *)
end

(** The offset, in bytes 0 to 7. *)
module Offset : sig
  val get : t -> int63
  val set : t -> int63 -> unit
end

(** The generation, in bytes 16 to 23. *)
module Generation : sig
  val get : t -> int63
  val set : t -> int63 -> unit
end

(** The fan, from byte 24. *)
module Fan : sig
  val get : t -> string
  (** [get t] reads the length of the fan, then the fan itself. *)

  val set : t -> string -> unit
  (** [set t s] writes the length of [s], then [s]. The data follows the fan, so
      [s] must have the length reserved with {!set_size}. *)

  val get_size : t -> int63
  (** [get_size t] is the length of the fan. *)

  val set_size : t -> int63 -> unit
  (** [set_size t n] writes the length of the fan only. It reserves [n] bytes
      for a fan written later with {!set}. *)
end

(** The first three fields of the header, read or written with a single system
    call. The fan is not read nor written. *)
module Header : sig
  type raw

  type t = {
    offset : int63;  (** The length of the valid data after the header. *)
    version : string;  (** The version, a string of 8 bytes. *)
    generation : int63;  (** The generation. *)
  }

  val get : raw -> t
  (** [get t] reads bytes 0 to 23. *)

  val set : raw -> t -> unit
  (** [set t h] writes bytes 0 to 23.

      @raise Invalid_argument if [h.version] is not 8 bytes long. *)
end
with type raw := t

(** The header of the files that only have an offset and a version, and whose
    data starts at byte 16, as in the legacy stores of irmin-pack up to 3.8.
    {!Generation}, {!Fan} and {!Header} must not be used on such files: the
    fields they access overlap the data. *)
module Header_prefix : sig
  type raw

  type t = {
    offset : int63;  (** The length of the valid data after the header. *)
    version : string;  (** The version, a string of 8 bytes. *)
  }

  val get : raw -> t
  (** [get t] reads bytes 0 to 15. *)

  val set : raw -> t -> unit
  (** [set t h] writes bytes 0 to 15.

      @raise Invalid_argument if [h.version] is not 8 bytes long. *)
end
with type raw := t
