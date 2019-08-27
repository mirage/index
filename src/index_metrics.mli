open Metrics

val duration_src : string -> (tags, ('a, exn) result -> data) src

module Instrument_IO (IO : Io.S) : Io.S
(** Takes an IO instance and instruments the methods with metric reporting *)
