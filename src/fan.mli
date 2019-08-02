type t

val v : hash_size:int -> entry_size:int -> int -> t

val search : t -> int -> int64 * int64

val update : t -> int -> int64 -> unit

val finalize : t -> unit
