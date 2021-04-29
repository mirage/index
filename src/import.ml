module Int63 = Optint.Int63

type int63 = Int63.t

module Logs = struct
  let default_reporter ?(prefix = "") () =
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
          prefix dt Logs_fmt.pp_header (level, h)
          Fmt.(styled `Magenta string)
          (Logs.Src.name src)
      in
      msgf @@ fun ?header ?tags fmt -> with_stamp header tags k fmt
    in
    { Logs.report }

  let setup ?(reporter = default_reporter ()) ?style_renderer ?level () =
    Fmt_tty.setup_std_outputs ?style_renderer ();
    Logs.set_level level;
    Logs.set_reporter reporter;
    ()

  open Cmdliner

  let ( let+ ) t f = Term.(const f $ t)
  let ( and+ ) a b = Term.(const (fun x y -> (x, y)) $ a $ b)

  let setup_term ?reporter () =
    let+ style_renderer = Fmt_cli.style_renderer ()
    and+ level = Logs_cli.level () in
    setup ?reporter ?style_renderer ?level ()

  include Logs
end
