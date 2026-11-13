(* Regression test for concurrent finds on distinct instances of the same
   [Index.Make] application. Each find prefetches a region of the data file in
   a buffer; this buffer must not be shared between instances, which are
   protected by distinct locks. *)

open Common

let root = Filename.concat "_tests" "unix.concurrent_find"
let nb_bindings = 2_000
let nb_rounds = 2

(* A merged instance holding [bindings], so that [find] reads the data file.
   The LRU is disabled so that every [find] reaches it. *)
let with_merged_instance name bindings f =
  let t =
    Index.v ~fresh:true ~lru_size:0 ~log_size:(2 * nb_bindings)
      (Filename.concat root name)
  in
  Fun.protect
    ~finally:(fun () -> Index.close t)
    (fun () ->
      List.iter (fun (k, v) -> Index.replace t k v) bindings;
      Index.try_merge_aux ~force:true t |> Index.await |> check_completed;
      f t)

(* The number of [bindings] that [find] misses or gets wrong in [t]. *)
let find_errors t bindings =
  let errors = ref 0 in
  for _ = 1 to nb_rounds do
    List.iter
      (fun (k, v) ->
        match Index.find t k with
        | v' when String.equal v v' -> ()
        | _ | (exception Not_found) -> incr errors)
      bindings
  done;
  !errors

(* Both instances hold the same keys with different values, so that reading
   the entries of the other instance gives wrong values, not only misses. *)
let two_instances_two_domains () =
  let keys = List.init nb_bindings (fun _ -> Key.v ()) in
  let bindings1 = List.map (fun k -> (k, Value.v ())) keys in
  let bindings2 = List.map (fun k -> (k, Value.v ())) keys in
  with_merged_instance "a" bindings1 @@ fun t1 ->
  with_merged_instance "b" bindings2 @@ fun t2 ->
  let d = Domain.spawn (fun () -> find_errors t2 bindings2) in
  let errors2 = ref 0 in
  let errors1 =
    Fun.protect
      ~finally:(fun () -> errors2 := Domain.join d)
      (fun () -> find_errors t1 bindings1)
  in
  Alcotest.(check int) "errors on the first instance" 0 errors1;
  Alcotest.(check int) "errors on the second instance" 0 !errors2

let tests =
  [ ("two instances, two domains", `Quick, two_instances_two_domains) ]
