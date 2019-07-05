(** Deudex

    deudex is a scalable implementation of persistent indexes in Ocaml.

    deudex is append-only, which means it provides [append], [find] and [mem]
    primitives.
    Multiples IOs are created when using the index :
    - A `log` IO contains all the recently added bindings, it is also kept in
      memory.
    - When the `log` IO is full, it is merged into multiple `index` IOs.
    Search is done first in `log` then in `index`, which makes recently added
    bindings search faster.
*)

(** The input of [Make] for keys. *)
module type Key = sig
  type t
  (** The type for keys. *)

  val equal : t -> t -> bool
  (** The equality function for keys. *)

  val hash : t -> int
  (** Note: Unevenly distributed hash functions may result in performance
      drops. *)

  val encode : t -> string
  (** [encode] is an encoding function. The encoded resulting values must be of
      fixed size. *)

  val decode : string -> int -> t
  (** [decode] is a decoding function such that [decode (encode t) 0 = t]. *)

  val encoded_size : int
  (** [encoded_size] is the size of the encoded keys, expressed in number of
      bytes. *)
end

(** The input of [Make] for values. The same requirements as for [Key]
    applies. *)
module type Value = sig
  type t

  val encode : t -> string

  val decode : string -> int -> t

  val encoded_size : int
end

module type IO = Io.S

exception RO_Not_Allowed
(** The exception raised when illegal operation is attempted on a read_only
    index. *)

(** Index module signature.  *)
module type S = sig
  type t
  (** The type for indexes. *)

  type key
  (** The type for keys. *)

  type value
  (** The type for values. *)

  val v :
    ?fresh:bool ->
    ?read_only:bool ->
    log_size:int ->
    fan_out_size:int ->
    string ->
    t
  (** The constructor for indexes.
      @param fresh
      @param read_only whether read-only mode is enabled for this index.
      @param log_size  the maximum number of bindings in the `log` IO.
      @param fan_out_size the size of the fan out for index IOs. Has to be a pozer of 2.
      *)

  val clear : t -> unit
  (** [clear t] clears [t] so that there are no more bindings in it. *)

  val find : t -> key -> value option
  (** [find t k] is the last binding of [k] in [t], if any. *)

  val mem : t -> key -> bool
  (** [mem t k] is [true] iff [k] is binded in [t]. *)

  val replace : t -> key -> value -> unit
  (** [replace t k v] binds [k] to [v] in [t], replacing the existing binding
      if any. *)

  val flush : t -> unit
  (** Flushes all buffers to the disk. *)
end

module Make (K : Key) (V : Value) (IO : IO) :
  S with type key = K.t and type value = V.t
