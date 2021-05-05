(*
 * Copyright (c) 2018-2021 Tarides <contact@tarides.com>
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

module Seq = struct
  include Seq

  (* Backported from ocaml 4.11 *)
  let rec unfold f u () =
    match f u with None -> Nil | Some (x, u') -> Cons (x, unfold f u')
end

let with_timer f =
  let started = Mtime_clock.counter () in
  let a = f () in
  let duration = Mtime_clock.count started in
  (a, duration)

let with_progress_bar ~sampling_interval ~message ~n ~unit =
  let bar =
    let w =
      if n = 0 then 1
      else float_of_int n |> log10 |> floor |> int_of_float |> succ
    in
    let pp fmt i = Format.fprintf fmt "%*Ld/%*d %s" w i w n unit in
    let pp f = f ~width:(w + 1 + w + 1 + String.length unit) pp in
    Progress_unix.counter ~sampling_interval ~mode:`ASCII ~width:79
      ~total:(Int64.of_int n) ~message ~pp ()
  in
  Progress_unix.with_reporters bar

module FSHelper = struct
  let file f =
    try (Unix.stat f).st_size with Unix.Unix_error (Unix.ENOENT, _, _) -> 0

  let index root =
    let index_dir = Filename.concat root "index" in
    let a = file (Filename.concat index_dir "data") in
    let b = file (Filename.concat index_dir "log") in
    let c = file (Filename.concat index_dir "log_async") in
    (a + b + c) / 1024 / 1024

  let size root = index root
  let get_size root = size root

  let rm_dir root =
    if Sys.file_exists root then (
      let cmd = Printf.sprintf "rm -rf %s" root in
      Logs.info (fun l -> l "exec: %s" cmd);
      ignore (Sys.command cmd : int))
end
