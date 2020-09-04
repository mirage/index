type 'a t
(** An ['a t] is a mutable handle on an ['a]. The handle is initially open and
    can be permenantly closed with {!close}. *)

val v : 'a -> 'a t
(** [v x] is a closeable handle on [x]. *)

val close : _ t -> unit
(** [close t] closes the handle [t]. No-op on an already-closed handle. *)

val get : 'a t -> [ `Open of 'a | `Closed ]
(** [get t] gets the current state of the handle [t]. *)

exception Closed

val get_exn : 'a t -> 'a
(** [get_exn t] tries to get the value referenced by the handle [t].

    @raise Closed if the handle has been {!close}d *)
