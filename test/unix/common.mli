val random_char : unit -> char

val report : unit -> unit
(** Set logs reporter at [Logs.Debug] level *)

module Log : Logs.LOG

module Default : sig
  val log_size : int
  val size : int
end

(** Simple key/value modules with String type and a random constructor *)
module Key : sig
  include Index.Key.S with type t = string

  val v : unit -> t
  val pp : t Fmt.t
end

module Value : sig
  include Index.Value.S with type t = string

  val v : unit -> t
  val pp : t Fmt.t
end

module Tbl : sig
  val v : size:int -> (Key.t, Value.t) Hashtbl.t
  (** Construct a new table of random key-value pairs. *)

  val check_binding : (Key.t, Value.t) Hashtbl.t -> Key.t -> Value.t -> unit
  (** Check that a binding exists in the table. *)
end

module Index : sig
  open Index.Private
  include S with type key = Key.t and type value = Value.t

  val replace_random :
    ?hook:[ `Merge of merge_stages ] Hook.t ->
    t ->
    (key * value) * merge_result async option
  (** Add a random fresh binding to the given index. *)

  val check_binding : t -> Key.t -> Value.t -> unit
  (** Check that a binding exists in the index.*)

  val check_not_found : t -> Key.t -> unit
  (** Check that a key does not exist in the index. *)
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

  val ignore : t -> unit

  val fresh_name : string -> string
  (** [fresh_name typ] is a clean directory for a resource of type [typ]. *)

  val with_empty_index :
    ?log_size:int ->
    ?lru_size:int ->
    ?flush_callback:(unit -> unit) ->
    ?throttle:[ `Overcommit_memory | `Block_writes ] ->
    unit ->
    (t -> 'a) ->
    'a
  (** [with_empty_index f] applies [f] to a fresh empty index. Afterwards, the
      index and any clones are closed. *)

  val with_full_index :
    ?log_size:int ->
    ?lru_size:int ->
    ?flush_callback:(unit -> unit) ->
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

val ( >> ) : ('a -> 'b) -> ('b -> 'c) -> 'a -> 'c
val uncurry : ('a -> 'b -> 'c) -> 'a * 'b -> 'c
val ignore_value : Value.t -> unit
val ignore_bool : bool -> unit
val ignore_index : Index.t -> unit

type binding = Key.t * Value.t

val pp_binding : binding Fmt.t

val check_completed :
  ([ `Aborted | `Completed ], [ `Async_exn of exn ]) result -> unit

val check_equivalence : Index.t -> (Key.t, Value.t) Hashtbl.t -> unit
val check_disjoint : Index.t -> (Key.t, Value.t) Hashtbl.t -> unit
val get_open_fd : string -> [> `Ok of string list | `Skip of string ]
val partition : string -> string list -> string list * string list
val check_entry : find:(string -> string) -> string -> string -> string -> unit
