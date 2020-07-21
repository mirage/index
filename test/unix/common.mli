val random_char : unit -> char

val report : unit -> unit
(** Set logs reporter at [Logs.Debug] level *)

module Log : Logs.LOG

(** Simple key/value modules with String type and a random constructor *)
module Key : sig
  include Index.Key with type t = string

  val v : unit -> t
end

module Value : sig
  include Index.Value with type t = string

  val v : unit -> t
end

module Index : sig
  include Index.Private.S with type key = Key.t and type value = Value.t

  val replace_random : t -> key * value
  (** Add a random fresh binding to the given index. *)
end

(** Helper constructors for fresh pre-initialised indices *)
module Make_context (Config : sig
  val root : string
end) : sig
  type t = private {
    rw : Index.t;
    tbl : (string, string) Hashtbl.t;
    clone : ?fresh:bool -> readonly:bool -> unit -> Index.t;
    close_all : unit -> unit;
  }

  val fresh_name : string -> string
  (** [fresh_name typ] is a clean directory for a resource of type [typ]. *)

  val with_empty_index :
    ?log_size:int ->
    ?auto_flush_callback:(unit -> unit) ->
    ?throttle:[ `Overcommit_memory | `Block_writes ] ->
    unit ->
    (t -> 'a) ->
    'a
  (** [with_empty_index f] applies [f] to a fresh empty index. Afterwards, the
      index and any clones are closed. *)

  val with_full_index :
    ?log_size:int ->
    ?auto_flush_callback:(unit -> unit) ->
    ?throttle:[ `Overcommit_memory | `Block_writes ] ->
    ?size:int ->
    unit ->
    (t -> 'a) ->
    'a
  (** [with_full_index f] applies [f] to a fresh index with a random table of
      key/value pairs. [f] also gets a constructor for opening clones of the
      index at the same location. Afterwards, the index and any clones are
      closed. *)
end

val ( let* ) : ('a -> 'b) -> 'a -> 'b
(** CPS monad *)

val ignore_value : Value.t -> unit

val ignore_bool : bool -> unit

val ignore_index : Index.t -> unit

val pp_binding : (Key.t * Value.t) Fmt.t

val check_completed :
  ([ `Aborted | `Completed ], [ `Async_exn of exn ]) result -> unit
