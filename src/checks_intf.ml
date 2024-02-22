open! Import

type empty = |

module type S = sig
  type io

  module Stat : sig
    val run : io:io -> root:string -> unit
    (** Read basic metrics from an existing store. *)

    val term : io:io -> (unit -> unit) Cmdliner.Term.t
    (** A pre-packaged [Cmdliner] term for executing {!run}. *)
  end

  module Integrity_check : sig
    val run : io:io -> root:string -> unit
    (** Check that the integrity invariants of a store are preserved, and
        display any broken invariants. *)

    val term : io:io -> (unit -> unit) Cmdliner.Term.t
    (** A pre-packaged [Cmdliner] term for executing {!run}. *)
  end

  val cli : io:io -> unit -> empty
  (** Run a [Cmdliner] binary containing tools for running offline integrity
      checks. *)
end

module type Platform_args = sig
  module IO : Io.S
  module Clock : Platform.CLOCK
  module Progress : Progress_engine.S
  module Fmt_tty : Platform.FMT_TTY
end

module type Checks = sig
  type nonrec empty = empty

  module type S = S
  module type Platform_args = Platform_args

  module Make (K : Data.Key) (V : Data.Value) (P : Platform_args) :
    S with type io = P.IO.io
end
