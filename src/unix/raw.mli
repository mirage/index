(** [Raw] wraps a file-descriptor with an file-format used internally by Index.
    The format contains the following header fields:

    - {b offset}: a 64-bit integer, denoting the length of the file containing
      valid data;
    - {b version}: an 8-byte version string;
    - {b generation}: a 64-bit integer denoting the generation number;
    - {b fan}: a 64-bit length field, followed by a string containing that many
      bytes. *)

type t
(** The type of [raw] file handles. *)

val v : Unix.file_descr -> t
(** Construct a [raw] value from a file descriptor. *)

val unsafe_write : t -> off:int64 -> string -> unit

val unsafe_read : t -> off:int64 -> len:int -> bytes -> int

val fsync : t -> unit

val close : t -> unit

module Version : sig
  val get : t -> string

  val set : t -> string -> unit
end

module Offset : sig
  val get : t -> int64

  val set : t -> int64 -> unit
end

module Generation : sig
  val get : t -> int64

  val set : t -> int64 -> unit
end

module Fan : sig
  val get : t -> string

  val set : t -> string -> unit

  val get_size : t -> int64

  val set_size : t -> int64 -> unit
end

type raw = t

module Header : sig
  type t = {
    offset : int64;  (** The length of the file containing valid data *)
    version : string;  (** Format version *)
    generation : int64;  (** Generation number *)
  }

  val get : raw -> t

  val set : raw -> t -> unit
end
