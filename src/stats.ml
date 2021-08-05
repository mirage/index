open! Import

type t = {
  mutable nb_merge : int;
  mutable merge_durations : float list;
  mutable nb_replace : int;
  mutable replace_durations : float list;
  mutable nb_sync : int;
  mutable time_sync : float;
}

let fresh_stats () =
  {
    nb_merge = 0;
    merge_durations = [];
    nb_replace = 0;
    replace_durations = [];
    nb_sync = 0;
    time_sync = 0.0;
  }

let stats = fresh_stats ()
let get () = stats

let reset_stats () =
  stats.nb_merge <- 0;
  stats.merge_durations <- [];
  stats.nb_replace <- 0;
  stats.replace_durations <- [];
  stats.nb_sync <- 0;
  stats.time_sync <- 0.0

let incr_nb_merge () = stats.nb_merge <- succ stats.nb_merge
let incr_nb_replace () = stats.nb_replace <- succ stats.nb_replace
let incr_nb_sync () = stats.nb_sync <- succ stats.nb_sync

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
