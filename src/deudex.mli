module type Key = sig
  type t

  val equal : t -> t -> bool

  val hash : t -> int

  val encode : t -> string

  val decode : string -> int -> t

  val encoded_size : int
end

module type Value = sig
  type t

  val encode : t -> string

  val decode : string -> int -> t

  val encoded_size : int
end

module type IO = Io.S

module Make (K : Key) (V : Value) (IO : IO) : sig
  type t

  val clear : t -> unit

  val v :
    ?fresh:bool ->
    log_size:int ->
    page_size:int ->
    pool_size:int ->
    fan_out_size:int ->
    string ->
    t

  val find : t -> K.t -> V.t option

  val mem : t -> K.t -> bool

  val append : t -> K.t -> V.t -> unit

  val flush : t -> unit
end
