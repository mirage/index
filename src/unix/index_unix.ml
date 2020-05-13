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

module Stats = Index.Stats

module Unix_IO = struct
  include Unix

  exception Error = Unix.Unix_error

  external pread : Unix.file_descr -> int64 -> bytes -> int -> int -> int
    = "caml_pread"

  external pwrite : Unix.file_descr -> int64 -> bytes -> int -> int -> int
    = "caml_pwrite"

  let read = pread

  let write = pwrite
end

module Mutex = struct
  include Mutex

  let with_lock t f =
    Mutex.lock t;
    try
      let ans = f () in
      Mutex.unlock t;
      ans
    with e ->
      Mutex.unlock t;
      raise e
end

module Thread = struct
  type t = Thread.t option

  let async f = Some (Thread.create f ())

  let yield = Thread.yield

  let return () = None

  let await t = match t with None -> () | Some t -> Thread.join t
end

module Make (K : Index.Key) (V : Index.Value) =
  Index.Make (K) (V) (Unix_IO) (Mutex) (Thread)

module Private = struct
  module Make (K : Index.Key) (V : Index.Value) =
    Index.Private.Make (K) (V) (Unix_IO) (Mutex) (Thread)
end
