type t

val v : int -> int -> t

val clear : t -> unit

val search : t -> int -> int64 * int64

val update : t -> int -> int64 -> unit

val flatten : t -> unit
