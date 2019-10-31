let src =
  Logs_threaded.enable ();
  Logs.Src.create "index" ~doc:"Index"

module Log = (val Logs.src_log src : Logs.LOG)

include Log
