module Test_eio = Test_io.Make (struct
  include Index_eio.Private.Platform

  let name = "eio"
end)

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun switch ->
  let io = { Index_eio.switch; root = Eio.Stdenv.cwd env } in
  Alcotest.run "index.eio" (Test_eio.tests ~io)
