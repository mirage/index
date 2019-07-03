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

module type S = sig
  type t

  type key

  type value

  val clear : t -> unit

  val v :
    ?fresh:bool ->
    log_size:int ->
    page_size:int ->
    pool_size:int ->
    fan_out_size:int ->
    string ->
    t

  val find : t -> key -> value option

  val mem : t -> key -> bool

  val append : t -> key -> value -> unit

  val flush : t -> unit
end

module Make (K : Key) (V : Value) (IO : IO) :
  S with type key = K.t and type value = V.t
