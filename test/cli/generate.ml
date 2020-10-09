module Size = struct
  let length = 20
end

let random () =
  let random_char _ = char_of_int (33 + Random.int 94) in
  String.init Size.length random_char

module Index =
  Index_unix.Make
    (Index.Key.String_fixed
       (Size))
       (Index.Value.String_fixed (Size))
       (Index.Cache.Noop)

let random () =
  let index = Index.v ~fresh:true ~log_size:100 "data/random" in
  for _ = 1 to 1001 do
    Index.replace index (random ()) (random ())
  done;
  Unix.sleep 1;
  Index.close index

let () = random ()
