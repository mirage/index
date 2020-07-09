type t = {
  mutable bytes_read : int;
  mutable nb_reads : int;
  mutable bytes_written : int;
  mutable nb_writes : int;
  mutable nb_merge : int;
  mutable merge_durations : float list;
  mutable nb_replace : int;
  mutable replace_durations : float list;
  mutable nb_sync : int;
  mutable time_sync : float;
}

let fresh_stats () =
  {
    bytes_read = 0;
    nb_reads = 0;
    bytes_written = 0;
    nb_writes = 0;
    nb_merge = 0;
    merge_durations = [];
    nb_replace = 0;
    replace_durations = [];
    nb_sync = 0;
    time_sync = 0.0;
  }

let stats = fresh_stats ()

let reset_stats () =
  stats.bytes_read <- 0;
  stats.nb_reads <- 0;
  stats.bytes_written <- 0;
  stats.nb_writes <- 0;
  stats.nb_merge <- 0;
  stats.merge_durations <- [];
  stats.nb_replace <- 0;
  stats.replace_durations <- [];
  stats.nb_sync <- 0;
  stats.time_sync <- 0.0

let get () = stats

let incr_bytes_read n = stats.bytes_read <- stats.bytes_read + n

let incr_bytes_written n = stats.bytes_written <- stats.bytes_written + n

let incr_nb_reads () = stats.nb_reads <- succ stats.nb_reads

let incr_nb_writes () = stats.nb_writes <- succ stats.nb_writes

let incr_nb_merge () = stats.nb_merge <- succ stats.nb_merge

let incr_nb_replace () = stats.nb_replace <- succ stats.nb_replace

let incr_nb_sync () = stats.nb_sync <- succ stats.nb_sync

let add_read n =
  incr_bytes_read n;
  incr_nb_reads ()

let add_write n =
  incr_bytes_written n;
  incr_nb_writes ()

let replace_timer = ref (Mtime_clock.counter ())

let nb_replace = ref 0

let start_replace () =
  if !nb_replace = 0 then replace_timer := Mtime_clock.counter ()

let end_replace ~sampling_interval =
  nb_replace := !nb_replace + 1;
  if !nb_replace = sampling_interval then (
    let span = Mtime_clock.count !replace_timer in
    let average = Mtime.Span.to_us span /. float_of_int !nb_replace in
    stats.replace_durations <- average :: stats.replace_durations;
    replace_timer := Mtime_clock.counter ();
    nb_replace := 0 )

let sync_with_timer f =
  let timer = Mtime_clock.counter () in
  f ();
  let span = Mtime_clock.count timer in
  stats.time_sync <- Mtime.Span.to_us span

let drop_head l = if List.length l >= 10 then List.tl l else l

let merge_with_timer f =
  let timer = Mtime_clock.counter () in
  let result = f () in
  let span = Mtime.Span.to_us (Mtime_clock.count timer) in
  stats.merge_durations <- drop_head stats.merge_durations @ [ span ];
  result
