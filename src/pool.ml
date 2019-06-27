module Make (IO : Io.S) = struct
  module Lru = Lru.Make (struct
    include Int64

    let hash = Hashtbl.hash
  end)

  type t = { pages : bytes Lru.t; length : int; size : int; io : IO.t }

  let v ~length ~size io =
    let pages = Lru.create size in
    { pages; length; io; size }

  let rec read t ~off ~len =
    let l = Int64.of_int t.length in
    let page_off = Int64.(mul (div off l) l) in
    let ioff = Int64.(to_int (sub off page_off)) in
    match Lru.find t.pages page_off with
    | buf ->
        if t.length - ioff < len then (
          Lru.remove t.pages page_off;
          (read [@tailcall]) t ~off ~len )
        else (buf, ioff)
    | exception Not_found ->
        let length = max t.length (ioff + len) in
        let length =
          if Int64.add page_off (Int64.of_int length) > IO.offset t.io then
            Int64.(to_int (sub (IO.offset t.io) page_off))
          else length
        in
        let buf = Bytes.create length in
        let n = IO.read t.io ~off:page_off buf in
        assert (n = length);
        Lru.add t.pages page_off buf;
        (buf, ioff)

  let trim ~off t =
    let max = Int64.(sub off (of_int t.length)) in
    Lru.filter (fun h _ -> h <= max) t.pages

  let clear t = Lru.clear t.pages
end
