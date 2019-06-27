module Make (IO : Io.S) : sig
  type t

  val v : length:int -> size:int -> IO.t -> t

  val read : t -> off:int64 -> len:int -> bytes * int

  val trim : off:int64 -> t -> unit

  val clear : t -> unit
end
