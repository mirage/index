open! Import

type _t = {
  bytes_read : int Atomic.t;
  nb_reads : int Atomic.t;
  bytes_written : int Atomic.t;
  nb_writes : int Atomic.t;
  nb_merge : int Atomic.t;
  merge_durations : float list Atomic.t;
  nb_replace : int Atomic.t;
  replace_durations : float list Atomic.t;
  nb_sync : int Atomic.t;
  time_sync : float Atomic.t;
  lru_hits : int Atomic.t;
  lru_misses : int Atomic.t;
}

let fresh_stats () =
  {
    bytes_read = Atomic.make 0;
    nb_reads = Atomic.make 0;
    bytes_written = Atomic.make 0;
    nb_writes = Atomic.make 0;
    nb_merge = Atomic.make 0;
    merge_durations = Atomic.make [];
    nb_replace = Atomic.make 0;
    replace_durations = Atomic.make [];
    nb_sync = Atomic.make 0;
    time_sync = Atomic.make 0.0;
    lru_hits = Atomic.make 0;
    lru_misses = Atomic.make 0;
  }

let stats = fresh_stats ()

let reset_stats () =
  Atomic.set stats.bytes_read 0;
  Atomic.set stats.nb_reads 0;
  Atomic.set stats.bytes_written 0;
  Atomic.set stats.nb_writes 0;
  Atomic.set stats.nb_merge 0;
  Atomic.set stats.merge_durations [];
  Atomic.set stats.nb_replace 0;
  Atomic.set stats.replace_durations [];
  Atomic.set stats.nb_sync 0;
  Atomic.set stats.time_sync 0.0;
  Atomic.set stats.lru_hits 0;
  Atomic.set stats.lru_misses 0

let incr_bytes_read n =
  Atomic.set stats.bytes_read (Atomic.get stats.bytes_read + n)

let incr_bytes_written n =
  Atomic.set stats.bytes_written (Atomic.get stats.bytes_written + n)

let incr_nb_reads () = Atomic.incr stats.nb_reads
let incr_nb_writes () = Atomic.incr stats.nb_writes
let incr_nb_merge () = Atomic.incr stats.nb_merge
let incr_nb_replace () = Atomic.incr stats.nb_replace
let incr_nb_sync () = Atomic.incr stats.nb_sync
let incr_nb_lru_hits () = Atomic.incr stats.lru_hits
let incr_nb_lru_misses () = Atomic.incr stats.lru_misses

let add_read n =
  incr_bytes_read n;
  incr_nb_reads ()

let add_write n =
  incr_bytes_written n;
  incr_nb_writes ()

type t = {
  bytes_read : int;
  nb_reads : int;
  bytes_written : int;
  nb_writes : int;
  nb_merge : int;
  merge_durations : float list;
  nb_replace : int;
  replace_durations : float list;
  nb_sync : int;
  time_sync : float;
  lru_hits : int;
  lru_misses : int;
}

let get () =
  {
    bytes_read = Atomic.get stats.bytes_read;
    nb_reads = Atomic.get stats.nb_reads;
    bytes_written = Atomic.get stats.bytes_written;
    nb_writes = Atomic.get stats.nb_writes;
    nb_merge = Atomic.get stats.nb_merge;
    merge_durations = Atomic.get stats.merge_durations;
    nb_replace = Atomic.get stats.nb_replace;
    replace_durations = Atomic.get stats.replace_durations;
    nb_sync = Atomic.get stats.nb_sync;
    time_sync = Atomic.get stats.time_sync;
    lru_hits = Atomic.get stats.lru_hits;
    lru_misses = Atomic.get stats.lru_misses;
  }

module Make (Clock : Platform.CLOCK) = struct
  let replace_timer = ref (Clock.counter ())
  let nb_replace = ref 0

  let start_replace () =
    if !nb_replace = 0 then replace_timer := Clock.counter ()

  let end_replace ~sampling_interval =
    nb_replace := !nb_replace + 1;
    if !nb_replace = sampling_interval then (
      let span = Clock.count !replace_timer in
      let average = Mtime.span_to_us span /. float_of_int !nb_replace in
      Atomic.set stats.replace_durations
        (average :: Atomic.get stats.replace_durations);
      replace_timer := Clock.counter ();
      nb_replace := 0)

  let sync_with_timer f =
    let timer = Clock.counter () in
    f ();
    let span = Clock.count timer in
    Atomic.set stats.time_sync (Mtime.span_to_us span)

  let drop_head l = if List.length l >= 10 then List.tl l else l

  let add_merge_duration span =
    let span = Mtime.span_to_us span in
    Atomic.set stats.merge_durations
      (drop_head (Atomic.get stats.merge_durations) @ [ span ])
end
