open! Import

type t = {
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
    nb_merge = 0;
    merge_durations = [];
    nb_replace = 0;
    replace_durations = [];
    nb_sync = 0;
    time_sync = 0.0;
    lru_hits = 0;
    lru_misses = 0;
  }

let stats = fresh_stats ()
let get () = stats

let reset () =
  stats.nb_merge <- 0;
  stats.merge_durations <- [];
  stats.nb_replace <- 0;
  stats.replace_durations <- [];
  stats.nb_sync <- 0;
  stats.time_sync <- 0.0;
  stats.lru_hits <- 0;
  stats.lru_misses <- 0

let incr_nb_merge () = stats.nb_merge <- succ stats.nb_merge
let incr_nb_replace () = stats.nb_replace <- succ stats.nb_replace
let incr_nb_sync () = stats.nb_sync <- succ stats.nb_sync
let incr_nb_lru_hits () = stats.lru_hits <- succ stats.lru_hits
let incr_nb_lru_misses () = stats.lru_misses <- succ stats.lru_misses

module Make (Clock : Platform.CLOCK) = struct
  let replace_timer = ref (Clock.counter ())
  let nb_replace = ref 0

  let start_replace () =
    if !nb_replace = 0 then replace_timer := Clock.counter ()

  let end_replace ~sampling_interval =
    nb_replace := !nb_replace + 1;
    if !nb_replace = sampling_interval then (
      let span = Clock.count !replace_timer in
      let average = Mtime.Span.to_us span /. float_of_int !nb_replace in
      stats.replace_durations <- average :: stats.replace_durations;
      replace_timer := Clock.counter ();
      nb_replace := 0)

  let sync_with_timer f =
    let timer = Clock.counter () in
    f ();
    let span = Clock.count timer in
    stats.time_sync <- Mtime.Span.to_us span

  let drop_head l = if List.length l >= 10 then List.tl l else l

  let add_merge_duration span =
    let span = Mtime.Span.to_us span in
    stats.merge_durations <- drop_head stats.merge_durations @ [ span ]
end

module Io_stats (R : Platform.RAW_STATS) = struct
  include R

  let tbl : (string, t) Hashtbl.t = Hashtbl.create 13

  let get_by_file file =
    try Hashtbl.find tbl file
    with Not_found ->
      let stats = R.fresh_stats () in
      Hashtbl.add tbl file stats;
      stats

  let get_all () =
    Hashtbl.fold (fun file stats acc -> (file, stats) :: acc) tbl []

  let bytes_read () =
    Hashtbl.fold (fun _file stats acc -> stats.R.bytes_read + acc) tbl 0

  let bytes_written () =
    Hashtbl.fold (fun _file stats acc -> stats.R.bytes_written + acc) tbl 0

  let nb_reads () =
    Hashtbl.fold (fun _file stats acc -> stats.R.nb_reads + acc) tbl 0

  let nb_writes () =
    Hashtbl.fold (fun _file stats acc -> stats.R.nb_writes + acc) tbl 0

  let get () =
    let acc = R.fresh_stats () in
    Hashtbl.iter
      (fun _file stats ->
        acc.bytes_read <- stats.bytes_read + acc.bytes_read;
        acc.R.nb_reads <- stats.R.nb_reads + acc.R.nb_reads;
        acc.R.bytes_written <- stats.R.bytes_written + acc.R.bytes_written;
        acc.R.nb_writes <- stats.R.nb_writes + acc.R.nb_writes)
      tbl;
    acc

  let reset_all () = Hashtbl.iter (fun _file stats -> R.reset stats) tbl
end
