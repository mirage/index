module type S = sig
  type t

  val v :
    readonly:bool ->
    fresh:bool ->
    generation:int64 ->
    fan_size:int64 ->
    string ->
    t

  val name : t -> string

  val offset : t -> int64

  val force_offset : t -> int64

  val readonly : t -> bool

  val read : t -> off:int64 -> len:int -> bytes -> int

  val clear : ?keep_generation:bool -> t -> unit

  val sync : t -> unit

  val version : t -> string

  val set_generation : t -> int64 -> unit

  val get_generation : t -> int64

  val set_fanout : t -> string -> unit

  val get_fanout : t -> string

  val rename : src:t -> dst:t -> unit

  val append : t -> string -> unit

  val close : t -> unit

  type lock

  val lock : string -> lock

  val unlock : lock -> unit

  exception Bad_Fd_Read

  module Mutex : sig
    type t

    val create : unit -> t

    val lock : t -> unit

    val unlock : t -> unit

    val with_lock : t -> (unit -> 'a) -> 'a
  end

  val async : (unit -> 'a) -> unit
end
