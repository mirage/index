module Test_unix = Test_io.Make (struct
  include Index_unix.Private.Platform

  let name = "unix"
end)

let () = Alcotest.run "index.unix" (Test_unix.tests ~io:())
