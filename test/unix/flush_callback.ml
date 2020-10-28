module I = Index
open Common
module Semaphore = Semaphore_compat.Semaphore.Binary

module Context = Common.Make_context (struct
  let root = Filename.concat "_tests" "unix.flush_callback"
end)

module Mutable_callback = struct
  type t = {
    flush_callback : unit -> unit;
    require_callback :
      'a. ?at_least_once:unit -> ?callback:(unit -> unit) -> (unit -> 'a) -> 'a;
        (** Locally override the definition of [flush_callback] inside a
            continuation. The default [callback] is the identity function.

            - The continuation must trigger the callback exactly once (unless
              [~at_least_once:()] is passed).
            - Any callbacks not scoped inside [require_callback] result in
              failure. *)
  }

  let v () : t =
    let unexpected () = Alcotest.fail "Callback call not expected" in
    let top = ref unexpected in
    let require_callback ?at_least_once ?(callback = fun () -> ()) (type a)
        (f : unit -> a) : a =
      let called = ref false in
      let prev_top = !top in
      (top :=
         fun () ->
           match (at_least_once, !called) with
           | None, true -> Alcotest.fail "flush_callback already triggered"
           | _, _ ->
               called := true;
               (* Ensure the callback does not recursively invoke an auto-flush. *)
               let saved_top = !top in
               top := unexpected;
               callback ();
               top := saved_top);
      let a = f () in
      if not !called then Alcotest.fail "flush_callback was not called";
      top := prev_top;
      a
    in
    let flush_callback () = !top () in
    { require_callback; flush_callback }
end

let check_no_merge binding = function
  | None -> binding
  | Some _merge_promise ->
      Alcotest.failf "New binding %a triggered an unexpected merge operation"
        pp_binding binding

(** Tests that [close] does not trigger the [flush_callback] *)
let test_close () =
  let fail typ () =
    Alcotest.failf "Closing <%s> should not trigger the flush_callback" typ
  in
  Context.with_empty_index ~flush_callback:(fail "empty index") ()
    Context.ignore;

  Context.with_full_index ~flush_callback:(fail "fresh index") () Context.ignore;

  let calls = ref 0 in
  Context.with_empty_index
    ~flush_callback:(fun () -> incr calls)
    ()
    (fun Context.{ rw; _ } ->
      Index.replace_random rw
      |> uncurry check_no_merge
      |> (ignore : binding -> unit));
  Alcotest.(check int)
    "Closing a dirty index should trigger the flush_callback once" 1 !calls

(** Test that [flush] triggers the [flush_callback] when necessary. *)
let test_flush () =
  let Mutable_callback.{ require_callback; flush_callback } =
    Mutable_callback.v ()
  in
  let* Context.{ rw; clone; _ } = Context.with_empty_index ~flush_callback () in
  let ro = clone ~readonly:true () in

  Index.flush rw (* No callback, since there are no bindings to persist *);
  let binding = Index.replace_random rw |> uncurry check_no_merge in
  require_callback
    ~callback:(fun () ->
      Log.app (fun m ->
          m "Checking that newly-added binding %a is not yet visible" pp_binding
            binding);
      Index.sync ro;
      Index.check_not_found ro (fst binding))
    (fun () -> Index.flush rw);

  Log.app (fun m ->
      m "After the flush, binding %a should be visible" pp_binding binding);
  Index.sync ro;
  uncurry (Index.check_binding ro) binding;

  let _ = Index.replace_random rw |> uncurry check_no_merge in
  Index.flush ~no_callback:() rw (* No callback, by user request *);

  ()

(** Test that flushes due to [replace] operations trigger the [flush_callback]:

    - 1. Initial flush of [log] before an automatic merge.
    - 2. Flushing of [log_async] while a merge is ongoing. *)
let test_replace () =
  let log_size = 8 in
  let bindings = Tbl.v ~size:log_size in
  let binding_list = bindings |> Hashtbl.to_seq |> List.of_seq in
  let Mutable_callback.{ require_callback; flush_callback } =
    Mutable_callback.v ()
  in
  let* Context.{ rw; clone; _ } =
    Context.with_empty_index ~log_size ~flush_callback ()
  in
  let ro = clone ~readonly:true () in

  (* The first [log_size]-many replaces don't trigger the callback. (Provided
     the [auto_flush_limit] is not reached, which it is not.) *)
  let replace_no_merge binding =
    Index.replace' rw (fst binding) (snd binding)
    |> check_no_merge binding
    |> (ignore : Key.t * Value.t -> unit)
  in
  binding_list |> List.iter replace_no_merge;

  (* The next replace overflows the log, causing the bindings to be persisted *)
  let do_merge = Semaphore.make false in
  let overflow_binding, merge_promise =
    require_callback
      ~callback:(fun () ->
        Log.app (fun m ->
            m
              "Checking newly-added bindings are not visible from a synced RO \
               instance until [flush_callback] is called");
        Index.sync ro;
        check_disjoint ro bindings)
      (fun () ->
        Index.replace_random
          ~hook:
            (I.Private.Hook.v (function
              | `Merge `Before -> Semaphore.acquire do_merge
              | _ -> ()))
          rw)
  in

  Log.app (fun m -> m "Checking merged bindings are now visible");
  Hashtbl.add bindings (fst overflow_binding) (snd overflow_binding);
  Index.sync ro;
  check_equivalence ro bindings;

  (* New values added during the merge go into [log_async] *)
  let async_binding = Index.replace_random rw |> uncurry check_no_merge in
  Log.app (fun m ->
      m "Added new binding %a while merge is ongoing" pp_binding async_binding);

  (* We could implicitly cause an automatic flush of [log_async], but it's
     simpler to just explicitly force one. *)
  Index.sync ro;
  require_callback ~at_least_once:()
    ~callback:(fun () ->
      Log.app (fun m ->
          m
            "Checking async_binding %a is not yet visible from a synced RO \
             instance"
            pp_binding async_binding);
      check_equivalence ro bindings)
    (fun () -> Index.flush rw);

  (* The merge triggers the callback when flushing [log_async] entries into
     [log]. (Not necessary here, since [log_async] values were already flushed.) *)
  require_callback (fun () ->
      Semaphore.release do_merge;
      merge_promise |> Option.get |> Index.await |> check_completed);

  Log.app (fun m ->
      m
        "Checking that all added bindings are now visible from a synced RO \
         instance");
  Hashtbl.add bindings (fst async_binding) (snd async_binding);
  Index.sync ro;
  check_equivalence ro bindings

let tests =
  [
    ("close", `Quick, test_close);
    ("flush", `Quick, test_flush);
    ("replace", `Quick, test_replace);
  ]
