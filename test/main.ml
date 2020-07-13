let () =
  Alcotest.run "index" [ ("cache", Cache.tests); ("search", Search.tests) ]
