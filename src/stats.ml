open! Import

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
  mutable lru_hits : int;
  mutable lru_misses : int;
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
    lru_hits = 0;
    lru_misses = 0;
  }

let fresh_stats_dls () = Domain.DLS.new_key (fun () -> fresh_stats ())
let kstats = fresh_stats_dls ()
let get () = Domain.DLS.get kstats

let reset_stats () =
  let stats = Domain.DLS.get kstats in
  stats.bytes_read <- 0;
  stats.nb_reads <- 0;
  stats.bytes_written <- 0;
  stats.nb_writes <- 0;
  stats.nb_merge <- 0;
  stats.merge_durations <- [];
  stats.nb_replace <- 0;
  stats.replace_durations <- [];
  stats.nb_sync <- 0;
  stats.time_sync <- 0.0;
  stats.lru_hits <- 0;
  stats.lru_misses <- 0

let incr_nb_merge () =
  let stats = Domain.DLS.get kstats in
  stats.nb_merge <- succ stats.nb_merge

let incr_nb_replace () =
  let stats = Domain.DLS.get kstats in
  stats.nb_replace <- succ stats.nb_replace

let incr_nb_sync () =
  let stats = Domain.DLS.get kstats in
  stats.nb_sync <- succ stats.nb_sync

let incr_nb_lru_hits () =
  let stats = Domain.DLS.get kstats in
  stats.lru_hits <- succ stats.lru_hits

let incr_nb_lru_misses () =
  let stats = Domain.DLS.get kstats in
  stats.lru_misses <- succ stats.lru_misses

let add_read n =
  let stats = Domain.DLS.get kstats in
  stats.bytes_read <- stats.bytes_read + n;
  stats.nb_reads <- succ stats.nb_reads

let add_write n =
  let stats = Domain.DLS.get kstats in
  stats.bytes_written <- stats.bytes_written + n;
  stats.nb_writes <- succ stats.nb_writes

let merge s1 s2 =
  {
    bytes_read = s1.bytes_read + s2.bytes_read;
    nb_reads = s1.nb_reads + s2.nb_reads;
    bytes_written = s1.bytes_written + s2.bytes_written;
    nb_writes = s1.nb_writes + s2.nb_writes;
    nb_merge = s1.nb_merge + s2.nb_merge;
    merge_durations = s1.merge_durations @ s2.merge_durations;
    nb_replace = s1.nb_replace + s2.nb_replace;
    replace_durations = s1.replace_durations @ s2.replace_durations;
    nb_sync = s1.nb_sync + s2.nb_sync;
    time_sync = s1.time_sync +. s2.time_sync;
    lru_hits = s1.lru_hits + s2.lru_hits;
    lru_misses = s1.lru_misses + s2.lru_misses;
  }

let merge_in s2 =
  let s1 = Domain.DLS.get kstats in
  s1.bytes_read <- s1.bytes_read + s2.bytes_read;
  s1.nb_reads <- s1.nb_reads + s2.nb_reads;
  s1.bytes_written <- s1.bytes_written + s2.bytes_written;
  s1.nb_writes <- s1.nb_writes + s2.nb_writes;
  s1.nb_merge <- s1.nb_merge + s2.nb_merge;
  s1.merge_durations <- s1.merge_durations @ s2.merge_durations;
  s1.nb_replace <- s1.nb_replace + s2.nb_replace;
  s1.replace_durations <- s1.replace_durations @ s2.replace_durations;
  s1.nb_sync <- s1.nb_sync + s2.nb_sync;
  s1.time_sync <- s1.time_sync +. s2.time_sync;
  s1.lru_hits <- s1.lru_hits + s2.lru_hits;
  s1.lru_misses <- s1.lru_misses + s2.lru_misses

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
      let stats = Domain.DLS.get kstats in
      stats.replace_durations <- average :: stats.replace_durations;
      replace_timer := Clock.counter ();
      nb_replace := 0)

  let sync_with_timer f =
    let timer = Clock.counter () in
    f ();
    let span = Clock.count timer in
    let stats = Domain.DLS.get kstats in
    stats.time_sync <- Mtime.span_to_us span

  let drop_head l = if List.length l >= 10 then List.tl l else l

  let add_merge_duration span =
    let span = Mtime.span_to_us span in
    let stats = Domain.DLS.get kstats in
    stats.merge_durations <- drop_head stats.merge_durations @ [ span ]
end
