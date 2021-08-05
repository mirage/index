type t = {
  mutable bytes_read : int;
  mutable nb_reads : int;
  mutable bytes_written : int;
  mutable nb_writes : int;
}

let fresh_stats () =
  { bytes_read = 0; nb_reads = 0; bytes_written = 0; nb_writes = 0 }

let stats = fresh_stats ()
let get () = stats

let reset_stats () =
  stats.bytes_read <- 0;
  stats.nb_reads <- 0;
  stats.bytes_written <- 0;
  stats.nb_writes <- 0

let incr_bytes_read n = stats.bytes_read <- stats.bytes_read + n
let incr_bytes_written n = stats.bytes_written <- stats.bytes_written + n
let incr_nb_reads () = stats.nb_reads <- succ stats.nb_reads
let incr_nb_writes () = stats.nb_writes <- succ stats.nb_writes

let add_read n =
  incr_bytes_read n;
  incr_nb_reads ()

let add_write n =
  incr_bytes_written n;
  incr_nb_writes ()
