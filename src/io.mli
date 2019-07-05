module type S = sig
  type t

  val v : ?read_only:bool -> string -> t

  val name : t -> string

  val offset : t -> int64

  val read : t -> off:int64 -> bytes -> int

  val clear : t -> unit

  val flush : t -> unit

  val rename : src:t -> dst:t -> unit

  val append : t -> string -> unit
end
