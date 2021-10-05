(*
 * Copyright (c) 2021 Tarides <contact@tarides.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open! Import

module Make (IO : Io.S) (Key : Data.Key) (Value : Data.Value) = struct
  module Entry = Data.Entry.Make (Key) (Value)

  module IO = struct
    include Io.Extend (IO)

    let iter_keys ?min f =
      let page_size = Int63.(mul Entry.encoded_sizeL (of_int 1_000)) in
      iter ~page_size ?min (fun ~off ~buf ~buf_off ->
          let key, _ = Entry.decode_key buf buf_off in
          f off key;
          Entry.encoded_size)
  end

  type t = {
    io : IO.t;  (** The disk file handler *)
    append_io : string -> unit;  (** Pre-allocated [IO.append io] closure *)
    mutable hashset : int63 Small_list.t Array.t;
        (** Hashset of offsets of entries in [io], keyed by the hash of the key
            stored at each offset. Length is always a power of two. *)
    mutable bucket_count_log2 : int;
        (** Invariant: equal to [log_2 (Array.length hashset)] *)
    mutable scratch_buf : bytes;
    mutable cardinal : int;
  }

  let io t = t.io
  let cardinal t = t.cardinal

  let clear_memory t =
    t.hashset <- [| Small_list.empty |];
    t.bucket_count_log2 <- 0;
    t.cardinal <- 0

  let clear ~generation ?hook ~reopen t =
    IO.clear ~generation ?hook ~reopen t.io;
    clear_memory t

  let close t =
    IO.close t.io;
    clear_memory t

  let flush ?no_callback ~with_fsync t = IO.flush ?no_callback ~with_fsync t.io

  let to_sorted_seq t =
    Array.to_seq t.hashset
    |> Seq.flat_map (fun bucket ->
           let arr =
             Small_list.to_array bucket
             |> Array.map (fun off ->
                    let n =
                      IO.read t.io ~off ~len:Entry.encoded_size t.scratch_buf
                    in
                    assert (n = Entry.encoded_size);
                    Entry.decode (Bytes.unsafe_to_string t.scratch_buf) 0)
           in
           Array.sort Entry.compare arr;
           Array.to_seq arr)

  let key_of_offset t off =
    let r = IO.read t.io ~off ~len:Key.encoded_size t.scratch_buf in
    assert (r = Key.encoded_size);
    fst (Entry.decode_key (Bytes.unsafe_to_string t.scratch_buf) 0)

  let entry_of_offset t off =
    let r = IO.read t.io ~off ~len:Entry.encoded_size t.scratch_buf in
    assert (r = Entry.encoded_size);
    Entry.decode (Bytes.unsafe_to_string t.scratch_buf) 0

  let elt_index t key =
    (* NOTE: we use the _uppermost_ bits of the key hash to index the bucket
       array, so that the hashset is approximately sorted by key hash (with only
       the entries within each bucket being relatively out of order). *)
    let unneeded_bits = Key.hash_size - t.bucket_count_log2 in
    (Key.hash key lsr unneeded_bits) land ((1 lsl t.bucket_count_log2) - 1)

  let resize t =
    (* Scale the number of hashset buckets. *)
    t.bucket_count_log2 <- t.bucket_count_log2 + 1;
    let new_bucket_count = 1 lsl t.bucket_count_log2 in
    if new_bucket_count > Sys.max_array_length then
      Fmt.failwith
        "Log_file.resize: can't construct a hashset with %d buckets \
         (Sys.max_array_length = %d)"
        new_bucket_count Sys.max_array_length;
    let new_hashset = Array.make new_bucket_count Small_list.empty in
    ArrayLabels.iteri t.hashset ~f:(fun i bucket ->
        (* The bindings in this bucket will be split into two new buckets, using
           the next bit of [Key.hash] as a discriminator. *)
        let bucket_2i, bucket_2i_plus_1 =
          Small_list.to_list bucket
          |> List.partition (fun offset ->
                 let key = key_of_offset t offset in
                 let new_index = elt_index t key in
                 assert (new_index lsr 1 = i);
                 new_index land 1 = 0)
        in
        new_hashset.(2 * i) <- Small_list.of_list bucket_2i;
        new_hashset.((2 * i) + 1) <- Small_list.of_list bucket_2i_plus_1);
    t.hashset <- new_hashset

  (** Replace implementation that only updates in-memory state (and doesn't
      write the binding to disk). *)
  let replace_memory t key offset =
    if t.cardinal > 2 * Array.length t.hashset then resize t;
    let elt_idx = elt_index t key in
    let bucket = t.hashset.(elt_idx) in
    let bucket =
      let key_found = ref false in
      let bucket' =
        Small_list.map bucket ~f:(fun offset' ->
            if !key_found then
              (* We ensure there's at most one binding for a given key *)
              offset'
            else
              let key' = key_of_offset t offset' in
              match Key.equal key key' with
              | false -> offset'
              | true ->
                  (* New binding for this key *)
                  key_found := true;
                  offset)
      in
      match !key_found with
      | true ->
          (* We're replacing an existing value. No need to change [cardinal]. *)
          bucket'
      | false ->
          (* The existing bucket doesn't contain this key. *)
          t.cardinal <- t.cardinal + 1;
          Small_list.cons offset bucket
    in
    t.hashset.(elt_idx) <- bucket

  let replace t key value =
    let offset = IO.offset t.io in
    Entry.encode' key value t.append_io;
    replace_memory t key offset

  let sync_entries ~min t =
    IO.iter_keys ~min (fun offset key -> replace_memory t key offset) t.io

  let reload t =
    clear_memory t;
    sync_entries ~min:Int63.zero t

  let create io =
    let cardinal = Int63.(to_int_exn (IO.offset io / Entry.encoded_sizeL)) in
    let bucket_count_log2, bucket_count =
      let rec aux n_log2 n =
        if n >= cardinal then (n_log2, n)
        else if n * 2 > Sys.max_array_length then (n_log2, n)
        else aux (n_log2 + 1) (n * 2)
      in
      aux 4 16
    in
    let hashset = Array.make bucket_count Small_list.empty in
    let t =
      {
        io;
        append_io = IO.append io;
        hashset;
        bucket_count_log2;
        scratch_buf = Bytes.create Entry.encoded_size;
        cardinal;
      }
    in
    IO.iter_keys (fun offset key -> replace_memory t key offset) io;
    t

  let find t key =
    let elt_idx = elt_index t key in
    let bucket = t.hashset.(elt_idx) in
    Small_list.find_map bucket ~f:(fun offset ->
        (* We expect the keys to match most of the time, so we decode the
           value at the same time. *)
        let entry = entry_of_offset t offset in
        match Key.equal key entry.key with
        | false -> None
        | true -> Some entry.value)
    |> function
    | None -> raise Not_found
    | Some x -> x

  let fold t ~f ~init =
    ArrayLabels.fold_left t.hashset ~init ~f:(fun acc bucket ->
        Small_list.fold_left bucket ~init:acc ~f:(fun acc offset ->
            let entry = entry_of_offset t offset in
            f acc entry))

  let iter t ~f =
    ArrayLabels.iter t.hashset ~f:(fun bucket ->
        Small_list.iter bucket ~f:(fun offset -> f (entry_of_offset t offset)))
end
