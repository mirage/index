open Common

module Context = Common.Make_context (struct
  let root = Filename.concat "_tests" "unix.auto_flush_callback"
end)

let check_binding t (k, v) =
  Index.find t k |> Alcotest.(check string) "Binding expected" v

let check_no_binding t k =
  try
    let v = Index.find t k in
    Alcotest.failf "Binding %a found but not expected" pp_binding (k, v)
  with Not_found -> ()

type callback_stack = {
  with_callback : 'a. callback:(unit -> unit) -> (unit -> 'a) -> 'a;
  auto_flush_callback : unit -> unit;
}

let callback_stack () : callback_stack =
  let top = ref (fun () -> Alcotest.fail "callback call unexpected") in
  let with_callback ~callback (type a) (f : unit -> a) : a =
    let called = ref false in
    let prev_top = !top in
    (top :=
       fun () ->
         match !called with
         | true -> Alcotest.fail "auto_flush_callback already triggered"
         | false ->
             called := true;
             callback ());
    let a = f () in
    if not !called then Alcotest.fail "auto_flush_callback was not called";
    top := prev_top;
    a
  in
  let auto_flush_callback () = !top () in
  { with_callback; auto_flush_callback }

(** Test that merges due to [replace] trigger the [auto_flush_callback]. *)
let test_replace () =
  let { with_callback; auto_flush_callback } = callback_stack () in
  let* Context.{ rw; clone; _ } =
    Context.with_empty_index ~log_size:4 ~auto_flush_callback ()
  in
  let ro = clone ~readonly:true () in

  (* Initial replaces don't trigger the callback *)
  let k1, v1 = Index.replace_random rw in
  let k2, v2 = Index.replace_random rw in
  let k3, v3 = Index.replace_random rw in
  let k4, v4 = Index.replace_random rw in

  (* Since [log_size = 4], a 5th [replace] operation causes all bindings to be flushed. *)
  let () =
    with_callback ~callback:(fun () ->
        Log.app (fun m ->
            m
              "Checking that newly-added bindings are not visible from a \
               synced RO instance until [auto_flush_callback] is called.");
        Index.sync ro;
        List.iter (check_no_binding ro) [ k1; k2; k3; k4 ])
    @@ fun () -> ignore (Index.replace_random rw)
  in

  let () =
    with_callback ~callback:(fun () ->
        Log.app (fun m -> m "Checking that merged bindings are now visible");
        Index.sync ro;
        List.iter (check_binding ro) [ (k1, v1); (k2, v2); (k3, v3); (k4, v4) ])
    @@ fun () -> Index.close rw
  in
  ()

let tests = [ ("replace", `Quick, test_replace) ]
