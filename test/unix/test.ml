module Tests =
  Index_test.Make (Index_unix.Private.IO) (Index_unix.Private.Lock)
    (Index_unix.Private.Mutex)
    (Index_unix.Private.Thread)

let () =
  Index_test.Common.report ();
  Alcotest.run "index-unix"
    (Tests.suite
    @ [
        ("io_array", Test_io_array.tests);
        ("force_merge", Test_force_merge.tests);
        ("flush_callback", Test_flush_callback.tests);
      ])
