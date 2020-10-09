module Size = struct
  let length = 20
end

module Index =
  Index_unix.Make
    (Index.Key.String_fixed
       (Size))
       (Index.Value.String_fixed (Size))
       (Index.Cache.Noop)

let () = match Index.Checks.cli () with _ -> .
