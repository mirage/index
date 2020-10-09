module type S = sig
  type t

  val v :
    ?flush_callback:(unit -> unit) ->
    readonly:bool ->
    fresh:bool ->
    generation:int64 ->
    fan_size:int64 ->
    string ->
    t

  val offset : t -> int64

  val read : t -> off:int64 -> len:int -> bytes -> int

  val clear : generation:int64 -> t -> unit

  val flush : ?no_callback:unit -> ?with_fsync:bool -> t -> unit

  val get_generation : t -> int64

  val set_fanout : t -> string -> unit

  val get_fanout : t -> string

  val rename : src:t -> dst:t -> unit

  val append : t -> string -> unit

  val close : t -> unit

  module Lock : sig
    type t

    val lock : string -> t

    val unlock : t -> unit

    val pp_dump : string -> (Format.formatter -> unit) option
    (** To be used for debugging purposes only. *)
  end

  module Header : sig
    type header = { offset : int64; generation : int64 }

    val set : t -> header -> unit

    val get : t -> header
  end

  val exists : string -> bool
  (** [exists name] is true iff there is a pre-existing IO instance called
      [name]. *)

  val size : t -> int
  (** Returns the true size of the underlying data representation in bytes. Note
      that this is not necessarily equal to the total size of {i observable}
      data, which is given by {!offset}.

      To be used for debugging purposes only. *)
end

module type Io = sig
  module type S = S

  module Extend (S : S) : sig
    include S with type t = S.t

    val iter :
      page_size:int64 ->
      ?min:int64 ->
      ?max:int64 ->
      (off:int64 -> buf:string -> buf_off:int -> int) ->
      t ->
      unit
  end
end
