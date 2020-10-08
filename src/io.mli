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

module type S = sig
  type t

  val v :
    ?flush_callback:(unit -> unit) ->
    readonly:bool ->
    fresh:bool ->
    generation:int64 ->
    fan_size:int64 ->
    string ->
    t

  val offset : t -> int64

  val read : t -> off:int64 -> len:int -> bytes -> int

  val clear : generation:int64 -> t -> unit

  val flush : ?no_callback:unit -> ?with_fsync:bool -> t -> unit

  val get_generation : t -> int64

  val set_fanout : t -> string -> unit

  val get_fanout : t -> string

  val rename : src:t -> dst:t -> unit

  val append : t -> string -> unit

  val close : t -> unit

  type lock

  val lock : string -> lock

  val unlock : lock -> unit

  module Header : sig
    type header = { offset : int64; generation : int64 }

    val set : t -> header -> unit

    val get : t -> header
  end

  val exists : string -> bool
  (** [exists name] is true iff there is a pre-existing IO instance called
      [name]. *)
end
