type t = {
  mutable bytes_read : int;
  mutable nb_reads : int;
  mutable bytes_written : int;
  mutable nb_writes : int;
  mutable nb_merge : int;
  mutable nb_replace : int;
  mutable replace_times : float list;
}
(** The type for stats for an index I.

    - [bytes_read] is the number of bytes read from disk;
    - [nb_reads] is the number of reads from disk;
    - [bytes_written] is the number of bytes written to disk;
    - [nb_writes] is the number of writes to disk;
    - [nb_merge] is the number of times a merge occurred;
    - [nb_replace] is the number of calls to [I.replace];
    - [replace_times] lists how much time replace operations took. Each element
      is an average of [n] consecutive replaces, where [n] is the
      [sampling_interval] specified when calling [end_replace]. *)

val reset_stats : unit -> unit

val get : unit -> t

val add_read : int -> unit

val add_write : int -> unit

val incr_nb_merge : unit -> unit

val incr_nb_replace : unit -> unit

val start_replace : unit -> unit

val end_replace : sampling_interval:int -> unit
