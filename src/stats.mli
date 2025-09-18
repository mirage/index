open! Import

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
(** The type for stats for an index I.

    - [bytes_read] is the number of bytes read from disk;
    - [nb_reads] is the number of reads from disk;
    - [bytes_written] is the number of bytes written to disk;
    - [nb_writes] is the number of writes to disk;
    - [nb_merge] is the number of times a merge occurred;
    - [merge_durations] lists how much time the last 10 merge operations took
      (in microseconds);
    - [nb_replace] is the number of calls to [I.replace];
    - [replace_durations] lists how much time replace operations took. Each
      element is an average of [n] consecutive replaces, where [n] is the
      [sampling_interval] specified when calling [end_replace].
    - [time_sync] is the duration of the latest call to sync. *)

val get : unit -> t
val reset_stats : unit -> unit
val add_read : int -> unit
val add_write : int -> unit
val incr_nb_merge : unit -> unit
val incr_nb_replace : unit -> unit
val incr_nb_sync : unit -> unit
val incr_nb_lru_hits : unit -> unit
val incr_nb_lru_misses : unit -> unit

module Make (_ : Platform.CLOCK) : sig
  val start_replace : unit -> unit
  val end_replace : sampling_interval:int -> unit
  val sync_with_timer : (unit -> unit) -> unit
  val add_merge_duration : Mtime.Span.t -> unit
end
