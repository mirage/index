type empty = |

module type S = sig
  val cli : unit -> empty
  (** Run a [Cmdliner] binary containing tools for running offline integrity
      checks. *)
end

module type Checks = sig
  type nonrec empty = empty

  module type S = S

  module Make (K : Data.Key) (V : Data.Value) (IO : Io.S) : S
end
