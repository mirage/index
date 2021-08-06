module Hook = Index.Private.Hook
open Common

let ( // ) = Filename.concat
let root = "_tests" // "unix.concurrent"
let src = Logs.Src.create "tests.unix.concurrent" ~doc:"Tests"

module Log = (val Logs.src_log src : Logs.LOG)

module Context = Common.Make_context (struct
  let root = root
end)

(* Helpers for Unix pipe *)
let write input =
  let m = Bytes.of_string (string_of_int (Unix.getpid ())) in
  let n = Unix.write input m 0 (Bytes.length m) in
  assert (n = Bytes.length m)

let read output =
  let buff = Bytes.create 5 in
  match Unix.read output buff 0 5 with
  | 0 -> Alcotest.fail "Something wrong when reading from the pipe"
  | n -> int_of_string (Bytes.to_string (Bytes.sub buff 0 n))

let ignore_pid (_ : int) = ()

let wait pid =
  let pid', status = Unix.waitpid [ Unix.WUNTRACED ] pid in
  if pid <> pid' then
    Alcotest.failf "I'm %d, expecting child %d, but got %d instead"
      (Unix.getpid ()) pid pid';
  match status with
  | Unix.WEXITED 0 -> Log.debug (fun l -> l "Child %d finished work" pid)
  | _ -> Alcotest.failf "Child %d died unexpectedly" pid

let lsof () =
  let name = "/tmp/" in
  let pid = string_of_int (Unix.getpid ()) in
  let fd_file = name ^ "tmp_" ^ pid in
  let lsof_command = "lsof -a -s -p " ^ pid ^ " >> " ^ fd_file in
  match Unix.system lsof_command with
  | Unix.WEXITED 0 -> ()
  | Unix.WEXITED _ ->
      failwith "failing `lsof` command. Is `lsof` installed on your system?"
  | Unix.WSIGNALED _ | Unix.WSTOPPED _ ->
      failwith "`lsof` command was interrupted"

module WriteBatch = struct
  let nb_workers = 1
  let nb_batch_writes = 1000
  let batch_size = 5

  let populate_array () =
    Array.init (batch_size * nb_batch_writes) (fun _ ->
        let k = Key.v () in
        let v = Value.v () in
        (k, v))

  let test_find_present ~ro t arr batch =
    let start_ = (batch - 1) * batch_size in
    let end_ = (batch_size * batch) - 1 in
    Fmt.epr "%b read %d from %d to %d\n%!" ro batch start_ end_;
    for i = start_ to end_ do
      let k, v = arr.(i) in
      let v' = Index.find t k in
      if not (v = v') then raise Not_found
    done

  let test_find_absent ~ro t arr batch =
    let start_ = (batch - 1) * batch_size in
    let end_ = (batch_size * batch) - 1 in
    Fmt.epr "%b read absent %d from %d to %d\n%!" ro batch start_ end_;
    for i = start_ to end_ do
      let k, _ = arr.(i) in
      match Index.find t k with
      | exception Not_found -> ()
      | _ -> Alcotest.failf "Found %s key after clear " k
    done

  let add_values t arr batch =
    Index.clear t;
    let start_ = (batch - 1) * batch_size in
    let end_ = (batch_size * batch) - 1 in
    Fmt.epr "add values %d to %d\n%!" start_ end_;
    for i = start_ to end_ do
      let k, v = arr.(i) in
      Index.replace t k v
    done;
    Index.flush t

  let worker input_write output_read name arr =
    let r = Index.v ~fresh:false ~log_size:4 ~readonly:true name in
    for i = 1 to nb_batch_writes do
      write input_write;
      ignore (read output_read);
      Index.sync r;
      test_find_present ~ro:true r arr i;
      Log.debug (fun l -> l "RO read by %d" (Unix.getpid ()))
    done;
    write input_write;
    ignore (read output_read);
    Index.sync r;
    test_find_absent ~ro:true r arr nb_batch_writes

  let concurrent_reads () =
    let output_write, input_write = Unix.pipe ()
    and output_read, input_read = Unix.pipe () in
    let root = Context.fresh_name "empty_index" in
    let arr = populate_array () in
    match Unix.fork () with
    | 0 ->
        Log.debug (fun l -> l "I'm %d" (Unix.getpid ()));
        worker input_write output_read root arr;
        exit 0
    | pid ->
        Log.debug (fun l ->
            l "I'm main process %d, created %d" (Unix.getpid ()) pid);
        let rw = Index.v ~fresh:true ~log_size:4 root in
        for i = 1 to nb_batch_writes do
          Log.debug (fun l -> l "Starting batch nb %d" i);
          if i mod 100 = 0 then lsof ();
          for _ = 0 to nb_workers - 1 do
            let pid = read output_write in
            Log.debug (fun l -> l "Ack from %d" pid)
          done;
          add_values rw arr i;
          for _ = 0 to nb_workers - 1 do
            write input_read
          done;
          test_find_present ~ro:false rw arr i;
          Log.debug (fun l -> l "Write from rw index")
        done;
        for _ = 0 to nb_workers - 1 do
          let pid = read output_write in
          Log.debug (fun l -> l "Ack from %d" pid)
        done;
        Index.clear rw;
        for _ = 0 to nb_workers - 1 do
          write input_read
        done;
        wait pid;
        Unix.close input_write;
        Unix.close output_write;
        Unix.close input_read;
        Unix.close output_read

  let check_os () = if Sys.os_type = "Win32" then () else concurrent_reads ()
  let tests = [ ("concurrent reads and writes", `Quick, check_os) ]
end

let () =
  (* Common.report (); *)
  Alcotest.run ~verbose:true "concurrent" [ ("write batch", WriteBatch.tests) ]
