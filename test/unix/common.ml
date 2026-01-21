let ( >> ) f g x = g (f x)
let src = Logs.Src.create "test/unix" ~doc:"Index_unix tests"

module Log = (val Logs.src_log src : Logs.LOG)

let report () =
  Logs_threaded.enable ();
  Index.Private.Logs.setup ~level:Logs.Debug
    (module Mtime_clock)
    (module Fmt_tty)

module String_size = struct
  let length = 20
end

let () = Random.self_init ()
let random_char () = char_of_int (33 + Random.int 94)
let random_string () = String.init String_size.length (fun _i -> random_char ())

module Default = struct
  let log_size = 4
  let lru_size = 0
  let size = 103
end

module Key = struct
  include Index.Key.String_fixed (String_size)

  let v = random_string
  let pp = Fmt.Dump.string
end

module Value = struct
  include Index.Value.String_fixed (String_size)

  let v = random_string
  let equal = String.equal
  let pp = Fmt.Dump.string
end

type binding = Key.t * Value.t

let pp_binding ppf (key, value) =
  Fmt.pf ppf "{ %a → %a }" (Repr.pp Key.t) key (Repr.pp Value.t) value

let check_entry ~find typ k v =
  match find k with
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

  let check_binding tbl = check_entry ~find:(Hashtbl.find tbl) "table"
end

module Index = struct
  include Index_unix.Private.Make (Key) (Value) (Index.Cache.Unbounded)

  let replace_random ?hook t =
    let ((key, value) as binding) = (Key.v (), Value.v ()) in
    (binding, replace' ?hook t key value)

  let check_binding index = check_entry ~find:(find index) "index"

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

  let empty_index ?(log_size = Default.log_size) ?(lru_size = Default.lru_size)
      ?flush_callback ?throttle () =
    let name = fresh_name "empty_index" in
    let cache = Index.empty_cache () in
    let rw =
      Index.v ?flush_callback ?throttle ~cache ~fresh:true ~log_size ~lru_size
        name
    in
    let close_all = ref (fun () -> Index.close rw) in
    let tbl = Hashtbl.create 0 in
    let clone ?(fresh = false) ~readonly () =
      let t =
        Index.v ?flush_callback ?throttle ~cache ~fresh ~log_size ~lru_size
          ~readonly name
      in
      (close_all := !close_all >> fun () -> Index.close t);
      t
    in
    { rw; tbl; clone; close_all = (fun () -> !close_all ()) }

  let full_index ?(size = Default.size) ?(log_size = Default.log_size)
      ?(lru_size = Default.lru_size) ?(flush_callback = fun () -> ()) ?throttle
      () =
    let f =
      (* Disable [flush_callback] while adding initial entries *)
      ref (fun () -> ())
    in
    let name = fresh_name "full_index" in
    let cache = Index.empty_cache () in
    let rw =
      Index.v
        ~flush_callback:(fun () -> !f ())
        ?throttle ~cache ~fresh:true ~log_size ~lru_size name
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
        Index.v ~flush_callback ?throttle ~cache ~fresh ~log_size ~lru_size
          ~readonly name
      in
      (close_all := !close_all >> fun () -> Index.close t);
      t
    in
    { rw; tbl; clone; close_all = (fun () -> !close_all ()) }

  let call_then_close (type a) (t : t) (f : t -> a) : a =
    let a = f t in
    t.close_all ();
    a

  let with_empty_index ?log_size ?lru_size ?flush_callback ?throttle () f =
    call_then_close
      (empty_index ?log_size ?lru_size ?flush_callback ?throttle ())
      f

  let with_full_index ?log_size ?lru_size ?flush_callback ?throttle ?size () f =
    call_then_close
      (full_index ?log_size ?lru_size ?flush_callback ?throttle ?size ())
      f
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

let get_open_fd root =
  let ( >>? ) x f = match x with `Ok x -> f x | `Skip err -> `Skip err in
  let pid = string_of_int (Unix.getpid ()) in
  let name = Filename.concat root "empty" in
  let fd_file = "index_fd_tmp" in
  let lsof_command = "lsof -a -s -p " ^ pid ^ " > " ^ fd_file in
  (match Sys.os_type with
  | "Unix" -> `Ok ()
  | _ -> `Skip "non-UNIX operating system")
  >>? fun () ->
  (match Unix.system lsof_command with
  | Unix.WEXITED 0 -> `Ok ()
  | Unix.WEXITED _ ->
      `Skip "failing `lsof` command. Is `lsof` installed on your system?"
  | Unix.WSIGNALED _ | Unix.WSTOPPED _ -> `Skip "`lsof` command was interrupted")
  >>? fun () ->
  let lines = ref [] in
  let extract_fd line =
    try
      let pos = Re.Str.search_forward (Re.Str.regexp name) line 0 in
      let fd = Re.Str.string_after line pos in
      lines := fd :: !lines
    with Not_found -> ()
  in
  let ic = open_in fd_file in
  (try
     while true do
       extract_fd (input_line ic)
     done
   with End_of_file -> close_in ic);
  `Ok !lines

let partition sub l =
  List.partition
    (fun line ->
      try
        ignore (Re.Str.search_forward (Re.Str.regexp sub) line 0);
        true
      with Not_found -> false)
    l
