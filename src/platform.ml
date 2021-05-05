module type CLOCK = sig
  (** A monotonic time source. See {!Mtime_clock} for an OS-dependent
      implementation. *)

  type counter

  val counter : unit -> counter
  val count : counter -> Mtime.span
end
