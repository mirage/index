val report : unit -> unit
(** Set logs reporter at [Logs.Info] level *)

module Make_context (Config : sig
  val key_size : int

  val hash_size : int

  val value_size : int
end) : sig
  module Key : sig
    include Index.Key with type t = string

    val v : unit -> t
  end

  module Value : sig
    include Index.Value with type t = string

    val v : unit -> t
  end
end

val with_timer : (unit -> 'a) -> 'a * float
(** [with_timer f] returns the execution time of f in seconds *)

val print_json : string -> (string * Yojson.Basic.t) list -> unit
