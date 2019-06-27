module Make (H : Hashtbl.HashedType) : sig
  type key = H.t

  type 'a t

  val create : int -> 'a t

  val clear : 'a t -> unit

  val add : 'a t -> key -> 'a -> unit

  val find : 'a t -> key -> 'a

  val remove : 'a t -> key -> unit

  val mem : 'a t -> key -> bool

  val filter : (key -> 'a -> bool) -> 'a t -> unit
end
