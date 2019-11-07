let reporter ?(prefix = "") () =
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let with_stamp h _tags k fmt =
      match level with
      | Logs.App -> Fmt.kpf k Fmt.stdout (fmt ^^ "@.%!")
      | _ ->
          let ppf = Fmt.stderr in
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
  Logs.set_level (Some Logs.App);
  Logs.set_reporter (reporter ())

let random_char () = char_of_int (33 + Random.int 94)

let random_string string_size =
  String.init string_size (fun _i -> random_char ())

module Make_context (Config : sig
  val key_size : int

  val hash_size : int

  val value_size : int
end) =
struct
  module Key = struct
    type t = string

    let v () = random_string Config.key_size

    let hash = Hashtbl.hash

    let hash_size = Config.hash_size

    let encode s = s

    let decode s off = String.sub s off Config.key_size

    let encoded_size = Config.key_size

    let equal = String.equal

    let pp s = Fmt.fmt "%s" s
  end

  module Value = struct
    type t = string

    let v () = random_string Config.value_size

    let encode s = s

    let decode s off = String.sub s off Config.value_size

    let encoded_size = Config.value_size

    let pp s = Fmt.fmt "%s" s
  end
end

let with_timer f =
  let t0 = Sys.time () in
  let x = f () in
  let t1 = Sys.time () -. t0 in
  (x, t1)
