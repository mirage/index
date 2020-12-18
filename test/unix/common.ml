let ( >> ) f g x = g (f x)

let reporter ?(prefix = "") () =
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let ppf = match level with Logs.App -> Fmt.stdout | _ -> Fmt.stderr in
    let with_stamp h _tags k fmt =
      let dt = Unix.gettimeofday () in
      Fmt.kpf k ppf
        ("%s%+04.0fus %a %a @[" ^^ fmt ^^ "@]@.")
        prefix dt
        Fmt.(styled `Magenta string)
        (Logs.Src.name src) Logs_fmt.pp_header (level, h)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k fmt
  in
  { Logs.report }

let src = Logs.Src.create "test/unix" ~doc:"Index_unix tests"

module Log = (val Logs.src_log src : Logs.LOG)

let report () =
  Logs_threaded.enable ();
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter (reporter ())

module String_size = struct
  let length = 20
end

let () = Random.self_init ()

let random_char () = char_of_int (33 + Random.int 94)

let random_string () = String.init String_size.length (fun _i -> random_char ())

module Key = struct
  include Index.Key.String_fixed (String_size)

  let v = random_string
end

module Value = struct
  include Index.Value.String_fixed (String_size)

  let v = random_string

  let equal = String.equal
end

type binding = Key.t * Value.t

let pp_binding ppf (key, value) =
  Fmt.pf ppf "{ %a → %a }" (Repr.pp Key.t) key (Repr.pp Value.t) value

let check_entry findf typ k v =
  match findf k with
  | v' when Value.equal v v' -> ()
  | v' (* v =/= v' *) ->
      Alcotest.failf "Found %s when checking for binding %a in %s" v' pp_binding
        (k, v) typ
  | exception Not_found ->
      Alcotest.failf "Expected key %s is missing in %s" k typ

module Tbl = struct
  let v ~size =
    let h = Hashtbl.create size in
    for _ = 1 to size do
      Hashtbl.add h (Key.v ()) (Value.v ())
    done;
    assert (Hashtbl.length h = size);
    h

  let check_binding tbl = check_entry (Hashtbl.find tbl) "table"
end

module Index = struct
  include Index_unix.Private.Make (Key) (Value) (Index.Cache.Unbounded)

  let replace_random ?hook t =
    let ((key, value) as binding) = (Key.v (), Value.v ()) in
    (binding, replace' ?hook t key value)

  let check_binding index = check_entry (find index) "index"

  let check_not_found index k =
    match find index k with
    | exception Not_found -> ()
    | v ->
        Alcotest.failf "Found binding %a but expected key to be absent"
          pp_binding (k, v)
end

let check_completed = function
  | Ok `Completed -> ()
  | Ok `Aborted -> Alcotest.fail "Unexpected asynchronous abort"
  | Error (`Async_exn exn) ->
      Alcotest.failf "Unexpected asynchronous exception: %s"
        (Printexc.to_string exn)

module Make_context (Config : sig
  val root : string
end) =
struct
  let fresh_name =
    let c = ref 0 in
    fun object_type ->
      incr c;
      let name = Filename.concat Config.root ("index_" ^ string_of_int !c) in
      Logs.info (fun m ->
          m "Constructing %s context object: %s" object_type name);
      name

  type t = {
    rw : Index.t;
    tbl : (string, string) Hashtbl.t;
    clone : ?fresh:bool -> readonly:bool -> unit -> Index.t;
    close_all : unit -> unit;
  }

  let ignore (_ : t) = ()

  let empty_index ?(log_size = 4) ?flush_callback ?throttle () =
    let name = fresh_name "empty_index" in
    let cache = Index.empty_cache () in
    let rw =
      Index.v ?flush_callback ?throttle ~cache ~fresh:true ~log_size name
    in
    let close_all = ref (fun () -> Index.close rw) in
    let tbl = Hashtbl.create 0 in
    let clone ?(fresh = false) ~readonly () =
      let t =
        Index.v ?flush_callback ?throttle ~cache ~fresh ~log_size ~readonly name
      in
      (close_all := !close_all >> fun () -> Index.close t);
      t
    in
    { rw; tbl; clone; close_all = (fun () -> !close_all ()) }

  let full_index ?(size = 103) ?(log_size = 4) ?(flush_callback = fun () -> ())
      ?throttle () =
    let f =
      (* Disable [flush_callback] while adding initial entries *)
      ref (fun () -> ())
    in
    let name = fresh_name "full_index" in
    let cache = Index.empty_cache () in
    let rw =
      Index.v
        ~flush_callback:(fun () -> !f ())
        ?throttle ~cache ~fresh:true ~log_size name
    in
    let close_all = ref (fun () -> Index.close rw) in
    let tbl = Hashtbl.create 0 in
    for _ = 1 to size do
      let k = Key.v () in
      let v = Value.v () in
      Index.replace rw k v;
      Hashtbl.replace tbl k v
    done;
    Index.flush rw;
    Index.try_merge_aux ~force:true rw |> Index.await |> check_completed;
    f := flush_callback (* Enable [flush_callback] *);
    let clone ?(fresh = false) ~readonly () =
      let t =
        Index.v ~flush_callback ?throttle ~cache ~fresh ~log_size ~readonly name
      in
      (close_all := !close_all >> fun () -> Index.close t);
      t
    in
    { rw; tbl; clone; close_all = (fun () -> !close_all ()) }

  let call_then_close (type a) (t : t) (f : t -> a) : a =
    let a = f t in
    t.close_all ();
    a

  let with_empty_index ?log_size ?flush_callback ?throttle () f =
    call_then_close (empty_index ?log_size ?flush_callback ?throttle ()) f

  let with_full_index ?log_size ?flush_callback ?throttle ?size () f =
    call_then_close (full_index ?log_size ?flush_callback ?throttle ?size ()) f
end

let ( let* ) f k = f k

let uncurry f (x, y) = f x y

let ignore_value (_ : Value.t) = ()

let ignore_bool (_ : bool) = ()

let ignore_index (_ : Index.t) = ()

let check_equivalence index htbl =
  Hashtbl.iter (Index.check_binding index) htbl;
  Index.iter (Tbl.check_binding htbl) index

let check_disjoint index htbl =
  Hashtbl.iter
    (fun k v ->
      match Index.find index k with
      | exception Not_found -> ()
      | v' when Value.equal v v' ->
          Alcotest.failf "Binding %a should not be present" pp_binding (k, v)
      | v' ->
          Alcotest.failf "Found value %a when checking for the absence of %a"
            (Repr.pp Value.t) v' pp_binding (k, v))
    htbl
