type t = { size : int; fans : int64 array; mask : int; shift : int }

let v hash_size n =
  let size = n in
  let nb_fans = 1 lsl size in
  let fans = Array.make nb_fans (-1L) in
  let shift = hash_size - size in
  let mask = (nb_fans - 1) lsl shift in
  { size; fans; mask; shift }

let fan t h = (h land t.mask) lsr t.shift

let clear t = Array.fill t.fans 0 (Array.length t.fans) (-1L)

let search t h =
  let fan = fan t h in
  let low = if fan = 0 then 0L else t.fans.(fan - 1) in
  (low, t.fans.(fan))

let update t hash off =
  let fan = fan t hash in
  t.fans.(fan) <- off

let flatten t =
  let rec loop curr i =
    if i = Array.length t.fans then ()
    else (
      if t.fans.(i) = -1L then t.fans.(i) <- curr;
      loop t.fans.(i) (i + 1) )
  in
  loop (-1L) 0
