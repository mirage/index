type t

val v : hash_size:int -> entry_size:int -> int -> t
(** [v ~hash_size ~entry_size n] creates a fan_out for an index with [hash_size]
    and [entry_size], containing [n] elements. *)

val search : t -> int -> int64 * int64
(** [search t hash] is the interval of offsets in witch [hash] is, if
    present. *)

val update : t -> int -> int64 -> unit
(** [update t hash off] updates [t] so that [hash] is registered to be at
    offset [off]. *)

val finalize : t -> unit
(** Finalizes the update of the fanout. This is mendatory before any [search]
    query. *)
