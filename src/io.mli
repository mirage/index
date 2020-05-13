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

module type Provider = sig
  type file_descr

  type file_perm = int

  type open_flag = Unix.open_flag

  type error = Unix.error

  type lock_command = Unix.lock_command

  exception Error of error * string * string

  val openfile : string -> open_flag list -> file_perm -> file_descr

  val close : file_descr -> unit

  val rename : string -> string -> unit

  val mkdir : string -> file_perm -> unit

  val unlink : string -> unit

  val read : file_descr -> int64 -> Bytes.t -> int -> int -> int

  val write : file_descr -> int64 -> Bytes.t -> int -> int -> int

  val single_write_substring : file_descr -> string -> int -> int -> int

  val fsync : file_descr -> unit

  val getpid : unit -> int

  val lockf : file_descr -> lock_command -> int -> unit
end

module type S = sig
  type t

  val v :
    readonly:bool ->
    fresh:bool ->
    generation:int64 ->
    fan_size:int64 ->
    string ->
    t

  val name : t -> string

  val offset : t -> int64

  val force_offset : t -> int64

  val readonly : t -> bool

  val read : t -> off:int64 -> len:int -> bytes -> int

  val clear : ?keep_generation:bool -> t -> unit

  val sync : ?with_fsync:bool -> t -> unit

  val version : t -> string

  val set_generation : t -> int64 -> unit

  val get_generation : t -> int64

  val set_fanout : t -> string -> unit

  val get_fanout : t -> string

  val rename : src:t -> dst:t -> unit

  val append : t -> string -> unit

  val close : t -> unit

  type lock

  val lock : string -> lock

  val unlock : lock -> unit
end

module Make : functor (_ : Provider) -> S
