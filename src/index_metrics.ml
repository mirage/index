open Metrics

(* There is currently no way of making metrics sinks cumulative, so here we
   manually hack around it by incrementing a reference *)
let duration_src name =
  let c = ref 0 in
  let data = function
    | Ok _ ->
        c := !c + 1;
        Data.v [ int "i" !c ]
    | Error e -> raise e
  in
  Src.v ~duration:true name ~tags:Tags.[] ~data

module Instrument_IO (IO : Io.S) : Io.S = struct
  include IO

  let bytes_read_count =
    let c = ref 0 in
    let data i =
      c := !c + i;
      Data.v [ int "bytes read" !c ]
    in
    Src.v "IO.bytes_read" ~tags:Tags.[] ~data

  let read_src = duration_src "IO.read"

  let read t ~off s =
    (fun () ->
      let bytes_read = read t ~off s in
      Metrics.add bytes_read_count (fun t -> t) (fun m -> m bytes_read);
      bytes_read)
    |> Metrics.run read_src (fun f -> f)
end
