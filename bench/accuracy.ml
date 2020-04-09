let () =
  let t1 = Sys.time () in
  while t1 = Sys.time () do
    ()
  done;
  Printf.printf "accuracy of sys time = %f" (Sys.time () -. t1)
