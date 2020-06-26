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

let report () =
  Logs_threaded.enable ();
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter (reporter ())

let string_size = 20

let () = Random.self_init ()

let random_char () = char_of_int (33 + Random.int 94)

let random_string () = String.init string_size (fun _i -> random_char ())

module Key = struct
  type t = string

  let v = random_string

  let hash = Hashtbl.hash

  let hash_size = 30

  let encode s = s

  let decode s off = String.sub s off string_size

  let encoded_size = string_size

  let equal = String.equal

  let pp s = Fmt.fmt "%s" s
end

module Value = struct
  type t = string

  let v = random_string

  let encode s = s

  let decode s off = String.sub s off string_size

  let encoded_size = string_size

  let pp s = Fmt.fmt "%s" s
end

module Index = Index_unix.Private.Make (Key) (Value)

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
  }

  let empty_index () =
    let name = fresh_name "empty_index" in
    let rw = Index.v ~fresh:true ~log_size:4 name in
    let tbl = Hashtbl.create 0 in
    let clone ?(fresh = false) ~readonly () =
      Index.v ~fresh ~log_size:4 ~readonly name
    in
    { rw; tbl; clone }

  let full_index ?(size = 103) () =
    let name = fresh_name "full_index" in
    let t = Index.v ~fresh:true ~log_size:4 name in
    let tbl = Hashtbl.create 0 in
    for _ = 1 to size do
      let k = Key.v () in
      let v = Value.v () in
      Index.replace t k v;
      Hashtbl.replace tbl k v
    done;
    Index.flush t;
    let clone ?(fresh = false) ~readonly () =
      Index.v ~fresh ~log_size:4 ~readonly name
    in
    { rw = t; tbl; clone }
end

let ignore_value (_ : Value.t) = ()

let ignore_bool (_ : bool) = ()

let ignore_index (_ : Index.t) = ()

let check_completed = function
  | Ok `Completed -> ()
  | Ok `Aborted -> Alcotest.fail "Unexpected asynchronous abort"
  | Error (`Async_exn exn) ->
      Alcotest.failf "Unexpected asynchronous exception: %s"
        (Printexc.to_string exn)
