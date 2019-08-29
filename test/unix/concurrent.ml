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

let string_size = 20

let index_size = 103

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

module Index = Index_unix.Make (Key) (Value)

let index_name = "hello"

let log_size = 4

let page_size = 2

let pool_size = 2

let t = Index.v ~fresh:true ~log_size index_name

(* [tbl] is the in-memory representation of the [t], i.e. tbl should always
contain the same elements as [t] *)
let tbl =
  let tbl = Hashtbl.create 0 in
  let rec loop i =
    if i = 0 then (
      Index.flush t;
      tbl )
    else
      let k = Key.v () in
      let v = Value.v () in
      Index.replace t k v;
      Hashtbl.replace tbl k v;
      loop (i - 1)
  in
  loop index_size

let rec random_new_key () =
  let r = Key.v () in
  if Hashtbl.mem tbl r then random_new_key () else r

let test_find_present t =
  Hashtbl.iter
    (fun k v ->
      match Index.find t k with
      | exception Not_found ->
          Alcotest.fail
            (Printf.sprintf "Wrong insertion: %s key is missing." k)
      | v' ->
          if not (v = v') then
            Alcotest.fail
              (Printf.sprintf "Wrong insertion: %s value is missing." v))
    tbl

let find_present_live () = test_find_present t

let live_tests = [ ("find (present)", `Quick, find_present_live) ]

let () =
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter (reporter ());
  Alcotest.run "index" [ ("concurrent", live_tests) ]
