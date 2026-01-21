(** [Raw] wraps a file-descriptor with an file-format used internally by Index; now it is
    is also used by irmin-pack's pack store (and maybe others)

The implementation uses a file descriptor, but uses space at the beginning of the file for
4 fields:

- {b offset}: a 64-bit integer, denoting the length of the file containing valid data;
  typically data is written to the end of a file, but not considered "valid" until a later
  fsync completes; in this module, there is no relation between the {!fsync} function, and
  the offset field - the offset field is uninterpreted and it is up to users of this
  module to set the offset appropriately

- {b version}: an 8-byte version string; in this module we don't impose any restrictions
  on this field, but elsewhere it is typically used to distinguish different formats of
  the file data (see irmin-pack's [Version] module - were the way to encode objects was
  changed from [`V1] to [`V2]; [Version.set t s] will use exactly the first 8 bytes of the
  version string; if the string is smaller there will be an error

- {b generation}: a 64-bit integer denoting the generation number; like [version], this is
  not interpreted any further in this module, but elsewhere it may be used, for example,
  as a counter to indicate that other RO processes need to reload data

- {b fan}: a 64-bit length field, followed by a string containing that many bytes; again,
  this is uninterpreted in this module, but is used by the Index's fan; note that this
  field means that the header does not have a fixed size

How does the user know where the header ends and the file data begins? One option is to
call {!fstat} after initializing the header info. In fact, in {!Index_unix} there is a
separate computation of the header size: [let header = eight ++ eight ++ eight ++ eight ++
fan_size]; presumably other users of this module do the same.
*)

open! Import

type t
(** The type of [raw] file handles. The implementation is just a plain
    {!Unix.file_descr}. *)

val v : Unix.file_descr -> t
(** Construct a [raw] value from a file descriptor; this just wraps the file descriptor;
    it does not read or write headers etc; for example, when called on an empty file the
    [v] constructor will not read or write any data to the file; it is up to the user to
    correctly initialize the headers. *)

val unsafe_write : t -> off:int63 -> string -> int -> int -> unit
(** [unsafe_write t ~off s soff len] writes data from [s] at position [soff] to the
    underlying file at position [off]; No account is made for the existence of the header
    - the user must make sure they write at offsets beyond the end of the header data. *)

val unsafe_read : t -> off:int63 -> len:int -> bytes -> int
(** [unsafe_read t ~off ~len buf] reads upto [len] bytes of data into [buf] and returns
    the number of bytes actually read. Again, no account is made for the existence of the
    header - the user must make sure they read at offsets beyond the end of the header
    data. *)

val fsync : t -> unit
val close : t -> unit
val fstat : t -> Unix.stats

(** An exception that can be raised when {b reading} (not writing!) header fields, in case
    the amount of bytes requested for a field is not the amount of bytes actually read
    from file. This presumably happens only for files that are not initialized correctly,
    i.e., they do not contain a full header. *)
(* FIXME rename this exception? *)
exception Not_written

module Version : sig
  val get : t -> string
  (** Return the version as a string of length 8 *)
  val set : t -> string -> unit
  (** [set t s] sets the version to [s]; uses first 8 bytes of [s]; error if [s]'s length
      is less than 8 *)
end

module Offset : sig
  val get : t -> int63
  val set : t -> int63 -> unit
end

module Generation : sig
  val get : t -> int63
  val set : t -> int63 -> unit
end

module Fan : sig
  val get : t -> string
  val set : t -> string -> unit
  val get_size : t -> int63
  val set_size : t -> int63 -> unit
end

module Header : sig
  type raw

  type t = {
    offset : int63;  (** The length of the file containing valid data *)
    version : string;  (** Format version *)
    generation : int63;  (** Generation number *)
  }

  val get : raw -> t
  val set : raw -> t -> unit
end
with type raw := t

(** Functions for interacting with the header format {i without} the generation
    number, provided for use in [irmin-pack]. *)
module Header_prefix : sig
  type raw

  type t = {
    offset : int63;  (** The length of the file containing valid data *)
    version : string;  (** Format version *)
  }

  val get : raw -> t
  val set : raw -> t -> unit
end
with type raw := t
