let src = Logs.Src.create "index-unix-test" ~doc:"Index Unix Testing"

module Log = (val Logs.src_log src : Logs.LOG)

include Log
