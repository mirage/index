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

open! Import

module type ELT = sig
  type t

  val encoded_size : int
  val decode : string -> int -> t
end

module type S = sig
  include Search.ARRAY

  type io

  val v : io -> t
end

module Make (IO : Io.S) (Elt : ELT) :
  S with type io = IO.t and type elt = Elt.t = struct
  module Elt = struct
    include Elt

    let encoded_sizeL = Int63.of_int encoded_size
  end

  type io = IO.t
  type elt = Elt.t

  module Buffers =
    Lru.M.Make
      (struct
        type t = int63 * int63

        let equal = ( = )
        let hash = Hashtbl.hash
      end)
      (struct
        type t = bytes

        let weight _ = 1
      end)

  type large = { io : IO.t; buffers : Buffers.t }
  type small = { low : int63; high : int63; buf : bytes }
  type t = Large of large | Small of small

  let cache_len = 100_000
  let v io = Large { io; buffers = Buffers.create cache_len }

  let get_entry_from_io io off =
    let buf = Bytes.create Elt.encoded_size in
    let n = IO.read io ~off ~len:Elt.encoded_size buf in
    assert (n = Elt.encoded_size);
    Elt.decode (Bytes.unsafe_to_string buf) 0

  let get_entry_from_buffer ~low buf off =
    let buf_off = Int63.(to_int_exn (off - low)) in
    assert (Bytes.length buf - buf_off >= Elt.encoded_size);
    Elt.decode (Bytes.unsafe_to_string buf) buf_off

  let get t i =
    let off = Int63.(i * Elt.encoded_sizeL) in
    match t with
    | Small t ->
        assert (t.low <= off && off <= t.high);
        get_entry_from_buffer ~low:t.low t.buf off
    | Large t -> get_entry_from_io t.io off

  let length t =
    let len =
      match t with
      | Small t -> Int63.(t.low + Int63.of_int (Bytes.length t.buf))
      | Large t -> IO.offset t.io
    in
    Int63.div len Elt.encoded_sizeL

  let with_buffer t (low, high) =
    let range = Int63.(to_int_exn (high - low)) in
    if range <= 0 then Large t
    else
      let buf = Bytes.create range in
      let n = IO.read t.io ~off:low ~len:range buf in
      assert (n = range);
      Buffers.add (low, high) buf t.buffers;
      Buffers.trim t.buffers;
      let buffer = { low; high; buf } in
      Small buffer

  let sub t ~low ~high =
    match t with
    | Small _ as t -> t
    | Large t as l -> (
        if Int63.compare low high > 0 then (
          Log.warn (fun m ->
              m "Requested pre-fetch region is empty: [%a, %a]" Int63.pp low
                Int63.pp high);
          l)
        else
          let low = Int63.(low * Elt.encoded_sizeL) in
          let high = Int63.((high + Int63.of_int 1) * Elt.encoded_sizeL) in
          let range = (low, high) in
          match Buffers.find range t.buffers with
          | Some buf -> Small { low; high; buf }
          | _ -> with_buffer t range)
end
