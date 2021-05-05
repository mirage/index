module Int63 = struct
  include Optint.Int63

  let ( + ) = add
  let ( - ) = sub
  let ( * ) = mul
  let ( / ) = div

  let to_int_exn =
    let max_int = of_int Int.max_int in
    fun x ->
      if compare x max_int > 0 then
        Fmt.failwith "Int63.to_int_exn: %a too large" pp x
      else to_int x

  let to_int_trunc = to_int
  let to_int = `shadowed
end

type int63 = Int63.t

module Logs = struct
  let default_reporter (type c) ?(prefix = "")
      (module Clock : Platform.CLOCK with type counter = c) (counter : c) =
    let report src level ~over k msgf =
      let k _ =
        over ();
        k ()
      in
      let ppf = match level with Logs.App -> Fmt.stdout | _ -> Fmt.stderr in
      let with_stamp h _tags k fmt =
        let dt = Mtime.Span.to_us (Clock.count counter) in
        Fmt.kpf k ppf
          ("%s%+04.0fus %a %a @[" ^^ fmt ^^ "@]@.")
          prefix dt Logs_fmt.pp_header (level, h)
          Fmt.(styled `Magenta string)
          (Logs.Src.name src)
      in
      msgf @@ fun ?header ?tags fmt -> with_stamp header tags k fmt
    in
    { Logs.report }

  let setup ?reporter ?style_renderer ?level (module Clock : Platform.CLOCK) =
    let start_time = Clock.counter () in
    let reporter =
      match reporter with
      | Some x -> x
      | None -> default_reporter (module Clock) start_time
    in
    Fmt_tty.setup_std_outputs ?style_renderer ();
    Logs.set_level level;
    Logs.set_reporter reporter;
    ()

  open Cmdliner

  let ( let+ ) t f = Term.(const f $ t)
  let ( and+ ) a b = Term.(const (fun x y -> (x, y)) $ a $ b)

  let setup_term ?reporter (module Clock : Platform.CLOCK) =
    let+ style_renderer = Fmt_cli.style_renderer ()
    and+ level = Logs_cli.level () in
    setup ?reporter ?style_renderer ?level (module Clock : Platform.CLOCK)

  include Logs
end
