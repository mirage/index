module Mutex = struct
  include Mutex

  let with_lock t f =
    Mutex.lock t;
    try
      let ans = f () in
      Mutex.unlock t;
      ans
    with e ->
      Mutex.unlock t;
      raise e

  let is_locked t =
    let locked = Mutex.try_lock t in
    if locked then Mutex.unlock t;
    not locked
end

module IO = Index.Platform.Memory.IO ()

module Lock = Index.Platform.Memory.Lock ()

module Tests = Index_test.Make (IO) (Lock) (Mutex) (Index.Thread.Identity)

let () =
  Index_test.Common.report ();
  Alcotest.run "index"
    ([ ("cache", Test_cache.tests); ("search", Test_search.tests) ]
    @ Tests.suite)
