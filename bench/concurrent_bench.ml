open Main

let read_index () =
  let r = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  Fmt.epr "\n Read in readonly index. \n";
  let count = ref 0 in
  Index.iter
    (fun _ _ ->
      count := !count + 1;
      if !count mod 1_000 = 0 then
        Fmt.epr "\r%a%!" pp_stats (!count, index_size))
    r

let () =
  let pid = Unix.fork () in
  if pid <> 0 then
    let _ = Unix.fork () in
    read_index ()
  else add_and_find ()
