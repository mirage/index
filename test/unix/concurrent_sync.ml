(* Regression test for a read-only instance that syncs in one domain while
   another domain calls [find] on it. A [find] running in the middle of the sync
   must not put an outdated value in the LRU.

   This test reliably detects a sync that clears the LRU before loading the new
   bindings without holding the lock of [find]. With the per-key invalidation
   of the LRU, the race window is much narrower, and a missing lock is only
   detected now and then. *)

open Common

let root = Filename.concat "_tests" "unix.concurrent_sync"
let nb_keys = 500
let nb_rounds = 20

(* Busy-waits until [cond ()] or [stop] holds. *)
let wait_until stop cond =
  while not (cond () || Atomic.get stop) do
    Domain.cpu_relax ()
  done

(* The number of [bindings] whose value is not the one [find] gives. *)
let outdated t bindings =
  List.fold_left
    (fun n (k, v) -> if String.equal v (Index.find t k) then n else n + 1)
    0 bindings

(* Each round updates all the keys, then syncs the read-only instance while
   another domain calls [find] on all the keys. Right after the sync, the
   read-only instance must give the new values. *)
let sync_and_find_two_domains () =
  let path = Filename.concat root "a" in
  (* The log is large enough to hold all the rounds: no merge happens. *)
  let log_size = 2 * nb_keys * (nb_rounds + 1) in
  let rw = Index.v ~fresh:true ~lru_size:0 ~log_size path in
  let ro = Index.v ~readonly:true ~lru_size:(2 * nb_keys) ~log_size path in
  Fun.protect ~finally:(fun () ->
      Index.close ro;
      Index.close rw)
  @@ fun () ->
  let keys = List.init nb_keys (fun _ -> Key.v ()) in
  let update () =
    let bindings = List.map (fun k -> (k, Value.v ())) keys in
    List.iter (fun (k, v) -> Index.replace rw k v) bindings;
    Index.flush rw;
    bindings
  in
  ignore (update () : (Key.t * Value.t) list);
  Index.sync ro;
  (* The rounds started by the main domain, and the ones in which the other
     domain is done with its finds. *)
  let started = Atomic.make 0 and found = Atomic.make 0 in
  let stop = Atomic.make false in
  let d =
    Domain.spawn (fun () ->
        (* If a [find] fails, stop the main domain too. *)
        Fun.protect ~finally:(fun () -> Atomic.set stop true) @@ fun () ->
        for round = 1 to nb_rounds do
          wait_until stop (fun () -> Atomic.get started >= round);
          List.iter (fun k -> ignore_value (Index.find ro k)) keys;
          Atomic.set found round
        done)
  in
  let errors =
    Fun.protect
      ~finally:(fun () ->
        Atomic.set stop true;
        Domain.join d)
      (fun () ->
        List.init nb_rounds succ
        |> List.fold_left
             (fun n round ->
               let bindings = update () in
               Atomic.set started round;
               Index.sync ro;
               wait_until stop (fun () -> Atomic.get found >= round);
               n + outdated ro bindings)
             0)
  in
  Alcotest.(check int) "outdated values after a sync" 0 errors

let tests =
  [ ("sync and find, two domains", `Quick, sync_and_find_two_domains) ]
