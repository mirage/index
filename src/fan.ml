type t = {
  hash_size : int;
  entry_size : int;
  size : int;
  fans : int64 array;
  mask : int;
  shift : int;
}

let log2 a = log a /. log 2.

let v ~hash_size ~entry_size n =
  let entry_sizef = float_of_int entry_size in
  let entries_per_page = 4096. /. entry_sizef in
  let entries_fan = float_of_int n /. (entries_per_page *. entries_per_page) in
  let size = max 0 (int_of_float (ceil (log2 entries_fan))) in
  let nb_fans = 1 lsl size in
  let shift = hash_size - size in
  {
    hash_size;
    entry_size;
    size;
    fans = Array.make nb_fans 0L;
    mask = (nb_fans - 1) lsl shift;
    shift;
  }

let fan t h = (h land t.mask) lsr t.shift

let search t h =
  let fan = fan t h in
  let low = if fan = 0 then 0L else t.fans.(fan - 1) in
  (low, t.fans.(fan))

let update t hash off =
  let fan = fan t hash in
  t.fans.(fan) <- off

let finalize t =
  let rec loop curr i =
    if i = Array.length t.fans then ()
    else (
      if t.fans.(i) = 0L then t.fans.(i) <- curr;
      loop t.fans.(i) (i + 1) )
  in
  loop 0L 0
