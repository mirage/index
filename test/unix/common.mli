val random_char : unit -> char

val report : unit -> unit
(** Set logs reporter at [Logs.Debug] level *)

(** Simple key/value modules with String type and a random constructor *)
module Key : sig
  include Index.Key with type t = string

  val v : unit -> t
end

module Value : sig
  include Index.Value with type t = string

  val v : unit -> t
end

module Index : Index.Private.S with type key = Key.t and type value = Value.t

(** Helper constructors for fresh pre-initialised indices *)
module Make_context (Config : sig
  val root : string
end) : sig
  type t = {
    rw : Index.t;
    tbl : (string, string) Hashtbl.t;
    clone : ?fresh:bool -> readonly:bool -> unit -> Index.t;
  }

  val fresh_name : string -> string
  (** [fresh_name typ] is a clean directory for a resource of type [typ]. *)

  val empty_index : unit -> t
  (** Fresh, empty index. *)

  val full_index : ?size:int -> unit -> t
  (** Fresh index with a random table of key/value pairs, and a given
      constructor for opening clones of the index at the same location. *)
end

val ignore_value : Value.t -> unit

val ignore_bool : bool -> unit

val ignore_index : Index.t -> unit

val check_completed :
  ([ `Aborted | `Completed ], [ `Async_exn of exn ]) result -> unit
