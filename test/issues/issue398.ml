(* See https://github.com/mirage/index/issues/398 *)

module L = struct
  let length = 4
end

module I =
  Index_unix.Make
    (Index.Key.String_fixed (L)) (Index.Value.String_fixed (L))
    (Index.Cache.Noop)

let () =
  let path = "issue398.index" in
  let t = I.v path ~log_size:16384 in
  let ro = I.v path ~readonly:true ~log_size:16384 in
  I.replace t "1234" "aaaa";
  I.flush t;
  I.sync ro;
  assert (I.find ro "1234" = "aaaa");
  I.replace t "1234" "aaab";
  I.flush t;
  I.sync ro;
  assert (I.find ro "1234" = "aaab")
