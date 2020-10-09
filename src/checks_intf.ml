type empty = |

module type S = sig
  val cli : unit -> empty
  (** Offline [fsck]-like utility for checking the integrity of Index stores
      built using this module. *)
end

module type Checks = sig
  type nonrec empty = empty

  module type S = S

  module Make (K : Data.Key) (V : Data.Value) (IO : Io.S) : S
end
