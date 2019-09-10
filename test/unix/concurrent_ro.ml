open Common

let index_name = Filename.concat "_tests" "unix.concurrent_ro"

let nb_workers = 2

let nb_batch_writes = 2

let batch_size = 5

let test_find_present t tbl =
  Hashtbl.iter
    (fun k v ->
      match Index.find t k with
      | exception Not_found ->
          Alcotest.failf "Wrong insertion: %s key is missing." k
      | v' ->
          if not (v = v') then
            Alcotest.failf "Wrong insertion: %s value is missing." v)
    tbl

let add_values t tbl =
  let rec loop i =
    if i = 0 then Index.flush t
    else
      let k = Key.v () in
      let v = Value.v () in
      Index.replace t k v;
      Hashtbl.replace tbl k v;
      loop (i - 1)
  in
  loop batch_size

let write input =
  try
    let m = Bytes.of_string (string_of_int (Unix.getpid ())) in
    ignore (Unix.write input m 0 (Bytes.length m))
  with Unix.Unix_error (n, f, arg) ->
    failwith (f ^ arg ^ Unix.error_message n)

let read output =
  let buff = Bytes.create 5 in
  match Unix.read output buff 0 5 with
  | 0 -> failwith "Something wrong when reading from the pipe"
  | n -> int_of_string (Bytes.to_string (Bytes.sub buff 0 n))

let worker input_write output_read tbl =
  let r = Index.v ~fresh:false ~readonly:true ~log_size index_name in
  test_find_present r tbl;
  for _i = 0 to nb_batch_writes do
    write input_write;
    ignore (read output_read);
    test_find_present r tbl;
    Logs.debug (fun l -> l "Read from ro index by %d" (Unix.getpid ()))
  done

let concurrent_reads () =
  let output_write, input_write = Unix.pipe ()
  and output_read, input_read = Unix.pipe () in
  let tbl = tbl index_name in
  let w = Index.v ~fresh:false ~log_size index_name in
  match Unix.fork () with
  | 0 ->
      for _i = 0 to nb_workers - 1 do
        match Unix.fork () with
        | 0 -> Logs.debug (fun l -> l "I'm %d" (Unix.getpid ()))
        | pid ->
            Logs.debug (fun l ->
                l "Child %d created by %d" pid (Unix.getpid ()));
            worker input_write output_read tbl;
            exit 0
      done;
      exit 0
  | _ ->
      for i = 0 to nb_batch_writes do
        Printf.printf "Starting batch nb %d\n%!" i;
        for _i = 0 to nb_workers - 1 do
          let pid = read output_write in
          Logs.debug (fun l -> l "Ack from %d" pid)
        done;
        add_values w tbl;
        for _i = 0 to nb_workers - 1 do
          write input_read
        done;
        test_find_present w tbl;
        Logs.debug (fun l -> l "Write from rw index")
      done;
      Unix.close input_write;
      Unix.close output_write;
      Unix.close input_read;
      Unix.close output_read

let tests = ("concurrent", [ ("concurrent reads", `Quick, concurrent_reads) ])

let () =
  Common.report ();
  Alcotest.run "index" [ tests ]
