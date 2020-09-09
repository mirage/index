(* The MIT License

Copyright (c) 2019 Craig Ferguson <craig@tarides.com>
                   Thomas Gazagnaire <thomas@tarides.com>
                   Ioana Cristescu <ioana@tarides.com>
                   Clément Pascutto <clement@tarides.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software. *)

module type Key = sig
  type t
  (** The type for keys. *)

  val equal : t -> t -> bool
  (** The equality function for keys. *)

  val hash : t -> int64
  (** Note: This function is required to be evenly distributed on 64 bits. *)

  val encode : t -> string
  (** [encode] is an encoding function. The resultant encoded values must have
      size {!encoded_size}. *)

  val encoded_size : int
  (** [encoded_size] is the size of the result of {!encode}, expressed in number
      of bytes. *)

  val decode : string -> int -> t
  (** [decode s off] is the decoded form of the encoded value at the offset
      [off] of string [s]. Must satisfy [decode (encode t) 0 = t]. *)

  val pp : t Fmt.t
  (** Formatter for keys *)
end

module type Value = sig
  type t

  val encode : t -> string

  val encoded_size : int

  val decode : string -> int -> t

  val pp : t Fmt.t
end

module type IO = Io.S

module type MUTEX = sig
  (** Locks for mutual exclusion *)

  type t
  (** The type of mutual-exclusion locks. *)

  val create : unit -> t
  (** Return a fresh mutex. *)

  val lock : t -> unit
  (** Lock the given mutex. Locks are not assumed to be re-entrant. *)

  val unlock : t -> unit
  (** Unlock the mutex. If any threads are attempting to lock the mutex, exactly
      one of them will gain access to the lock. *)

  val with_lock : t -> (unit -> 'a) -> 'a
  (** [with_lock t f] first obtains [t], then computes [f ()], and finally
      unlocks [t]. *)

  val is_locked : t -> bool
  (** [is_locked t] returns [true] if the mutex is locked, without locking [t]. *)
end

module type THREAD = sig
  (** Cooperative threads. *)

  type 'a t
  (** The type of thread handles. *)

  val async : (unit -> 'a) -> 'a t
  (** [async f] creates a new thread of control which executes [f ()] and
      returns the corresponding thread handle. The thread terminates whenever
      [f ()] returns a value or raises an exception. *)

  val await : 'a t -> ('a, [ `Async_exn of exn ]) result
  (** [await t] blocks on the termination of [t]. *)

  val return : 'a -> 'a t
  (** [return ()] is a pre-terminated thread handle. *)

  val yield : unit -> unit
  (** Re-schedule the calling thread without suspending it. *)
end

module type S = sig
  type t
  (** The type for indexes. *)

  type key
  (** The type for keys. *)

  type value
  (** The type for values. *)

  type cache
  (** The type for caches of index instances. *)

  val empty_cache : unit -> cache
  (** Construct a new empty cache of index instances. *)

  val v :
    ?flush_callback:(unit -> unit) ->
    ?cache:cache ->
    ?fresh:bool ->
    ?readonly:bool ->
    ?throttle:[ `Overcommit_memory | `Block_writes ] ->
    log_size:int ->
    string ->
    t
  (** The constructor for indexes.

      @param flush_callback A function to be called before any new bindings are
      persisted to disk (including both automatic flushing and explicit calls to
      {!flush} or {!close}).

      This can be used to ensure certain pre-conditions are met before bindings
      are persisted to disk. (For instance, if the index bindings are pointers
      into another data-structure [d], it may be necessary to flush [d] first to
      avoid creating dangling pointers.)
      @param cache used for instance sharing.
      @param fresh whether an existing index should be overwritten.
      @param read_only whether read-only mode is enabled for this index.
      @param throttle the strategy to use when the cache are full and and async
      in already in progress. [Block_writes] (the default) blocks any new writes
      until the merge is completed. [Overcommit_memory] does not block but
      continues to fill the (already full) cache.
      @param log_size the maximum number of bindings in the `log` IO. *)

  val clear : t -> unit
  (** [clear t] clears [t] so that there are no more bindings in it. *)

  val find : t -> key -> value
  (** [find t k] is the binding of [k] in [t]. *)

  val mem : t -> key -> bool
  (** [mem t k] is [true] iff [k] is bound in [t]. *)

  exception Invalid_key_size of key

  exception Invalid_value_size of value
  (** The exceptions raised when trying to add a key or a value of different
      size than encoded_size *)

  val replace : t -> key -> value -> unit
  (** [replace t k v] binds [k] to [v] in [t], replacing any existing binding of
      [k]. *)

  val filter : t -> (key * value -> bool) -> unit
  (** [filter t p] removes all the bindings (k, v) that do not satisfy [p]. This
      operation is costly and blocking. *)

  val iter : (key -> value -> unit) -> t -> unit
  (** Iterates over the index bindings. Limitations:

      - Order is not specified.
      - In case of recent replacements of existing values (since the last
        merge), this will hit both the new and old bindings.
      - May not observe recent concurrent updates to the index by other
        processes. *)

  val flush : ?no_callback:unit -> ?with_fsync:bool -> t -> unit
  (** Flushes all internal buffers of the [IO] instances.

      - Passing [~no_callback:()] disables calling the [flush_callback] passed
        to {!v}.
      - If [with_fsync] is [true], this also flushes the OS caches for each [IO]
        instance. *)

  val close : t -> unit
  (** Closes all resources used by [t], flushing any internal buffers in the
      instance. *)

  val sync : t -> unit
  (** [sync t] syncs a read-only index with the files on disk. Raises
      {!RW_not_allowed} if called by a read-write index. *)

  val is_merging : t -> bool
  (** [is_merging t] returns true if [t] is running a merge. Raises
      {!RO_not_allowed} if called by a read-only index. *)
end

module type Index = sig
  (** The input of {!Make} for keys. *)
  module type Key = sig
    (* N.B. We use [sig ... end] redirections to avoid linking to the [_intf]
       file in the generated docs. Once Odoc 2 is released, this can be
       removed. *)

    include Key
    (** @inline *)
  end

  (** The input of {!Make} for values. The same requirements as for {!Key}
      apply. *)
  module type Value = sig
    include Value
    (** @inline *)
  end

  module type IO = sig
    include Io.S
    (** @inline *)
  end

  module type MUTEX = sig
    include MUTEX
    (** @inline *)
  end

  module type THREAD = sig
    include THREAD
    (** @inline *)
  end

  (** Signatures and implementations of caches. {!Make} requires a cache in
      order to provide instance sharing. *)
  module Cache : sig
    include module type of Cache
    (** @inline *)
  end

  (** Index module signature. *)
  module type S = sig
    include S
    (** @inline *)
  end

  exception RO_not_allowed
  (** The exception raised when a write operation is attempted on a read_only
      index. *)

  exception RW_not_allowed
  (** The exception is raised when a sync operation is attempted on a read-write
      index. *)

  exception Closed
  (** The exception raised when any operation is attempted on a closed index,
      except for [close], which is idempotent. *)

  module Make
      (K : Key)
      (V : Value)
      (IO : IO)
      (M : MUTEX)
      (T : THREAD)
      (C : Cache.S) : S with type key = K.t and type value = V.t

  (** Run-time metric tracking for index instances. *)
  module Stats : sig
    include module type of Stats
    (** @inline *)
  end

  (** These modules should not be used. They are exposed purely for testing
      purposes. *)
  module Private : sig
    module Hook : sig
      type 'a t

      val v : ('a -> unit) -> 'a t
    end

    module Search : module type of Search

    module Io_array : module type of Io_array

    module Fan : module type of Fan

    type merge_stages = [ `After | `After_clear | `After_first_entry | `Before ]
    (** Some operations that trigger a merge can have hooks inserted at the
        following stages:

        - [`Before]: immediately before merging (while holding the merge lock);
        - [`After_clear]: immediately after clearing the log, at the end of a
          merge;
        - [`After_first_entry]: immediately after adding the first entry in the
          merge file, if the data file contains at least one entry;
        - [`After]: immediately after merging (while holding the merge lock). *)

    type merge_result = [ `Completed | `Aborted ]

    module type S = sig
      include S

      type 'a async
      (** The type of asynchronous computation. *)

      val replace' :
        ?hook:[ `Merge of merge_stages ] Hook.t ->
        t ->
        key ->
        value ->
        merge_result async option
      (** [replace' t k v] is like {!replace t k v} but returns a promise of a
          merge result if the {!replace} call triggered one. *)

      val close' : hook:[ `Abort_signalled ] Hook.t -> t -> unit
      (** [`Abort_signalled]: after the cancellation signal has been sent to any
          concurrent merge operations, but {i before} blocking on those
          cancellations having completed. *)

      val clear' : hook:[ `Abort_signalled ] Hook.t -> t -> unit

      val force_merge : ?hook:merge_stages Hook.t -> t -> merge_result async
      (** [force_merge t] forces a merge for [t]. *)

      val await : 'a async -> ('a, [ `Async_exn of exn ]) result
      (** Wait for an asynchronous computation to finish. *)

      val replace_with_timer :
        ?sampling_interval:int -> t -> key -> value -> unit
      (** Time replace operations. The reported time is an average on an number
          of consecutive operations, which can be specified by
          [sampling_interval]. If [sampling_interval] is not set, no operation
          is timed. *)

      val sync' :
        ?hook:[ `Before_offset_read | `After_offset_read ] Hook.t -> t -> unit
      (** Hooks:

          - [`Before_offset_read]: before reading the generation number and the
            offset.
          - [`After_offset_read]: after reading the generation number and
            offset. *)
    end

    module Make
        (K : Key)
        (V : Value)
        (IO : IO)
        (M : MUTEX)
        (T : THREAD)
        (C : Cache.S) : S with type key = K.t and type value = V.t
  end
end
