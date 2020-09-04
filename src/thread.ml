module type S = sig
  (** Cooperative threads. *)

  type 'a t
  (** The type of thread handles. *)

  val async : (unit -> 'a) -> 'a t
  (** [async f] creates a new thread of control which executes [f ()] and
      returns the corresponding thread handle. The thread terminates whenever
      [f ()] returns a value or raises an exception. *)

  val await : 'a t -> ('a, [ `Async_exn of exn ]) result
  (** [await t] blocks on the termination of [t]. *)

  val return : 'a -> 'a t
  (** [return ()] is a pre-terminated thread handle. *)

  val yield : unit -> unit
                        (** Re-schedule the calling thread without suspending it. *)
end

module Identity : S = struct
  type 'a t = 'a

  let async f = f ()

  let await t = Ok t

  let return x = x

  let yield () = ()
end
