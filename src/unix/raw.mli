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

val unsafe_write : t -> off:Int63.t -> string -> unit

val unsafe_read : t -> off:Int63.t -> len:int -> bytes -> int

val fsync : t -> unit

val close : t -> unit

val fstat : t -> Unix.stats

module Version : sig
  val get : t -> string

  val set : t -> string -> unit
end

module Offset : sig
  val get : t -> Int63.t

  val set : t -> Int63.t -> unit
end

module Generation : sig
  val get : t -> Int63.t

  val set : t -> Int63.t -> unit
end

module Fan : sig
  val get : t -> string

  val set : t -> string -> unit

  val get_size : t -> Int63.t

  val set_size : t -> Int63.t -> unit
end

module Header : sig
  type raw

  type t = {
    offset : Int63.t;  (** The length of the file containing valid data *)
    version : string;  (** Format version *)
    generation : Int63.t;  (** Generation number *)
  }

  val get : raw -> t

  val set : raw -> t -> unit
end
with type raw := t
