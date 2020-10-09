module type Key = sig
  type t
  (** The type for keys. *)

  val equal : t -> t -> bool
  (** The equality function for keys. *)

  val hash : t -> int
  (** Note: Unevenly distributed hash functions may result in performance drops. *)

  val hash_size : int
  (** The number of bits necessary to encode the maximum output value of
      {!hash}. `Hashtbl.hash` uses 30 bits.

      Overestimating the [hash_size] will result in performance drops;
      underestimation will result in undefined behavior. *)

  val encode : t -> string
  (** [encode] is an encoding function. The resultant encoded values must have
      size {!encoded_size}. *)

  val encoded_size : int
  (** [encoded_size] is the size of the result of {!encode}, expressed in number
      of bytes. *)

  val decode : string -> int -> t
  (** [decode s off] is the decoded form of the encoded value at the offset
      [off] of string [s]. Must satisfy [decode (encode t) 0 = t]. *)

  val pp : t Fmt.t
  (** Formatter for keys *)
end

module type Value = sig
  type t

  val encode : t -> string

  val encoded_size : int

  val decode : string -> int -> t

  val pp : t Fmt.t
end

module String_fixed (L : sig
  val length : int
end) : sig
  type t = string

  include Key with type t := string

  include Value with type t := string
end = struct
  type t = string

  let hash = Hashtbl.hash

  let hash_size = 30

  let encode s = s

  let decode s off = String.sub s off L.length

  let encoded_size = L.length

  let equal = String.equal

  let pp = Fmt.string
end
