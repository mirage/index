exception Closed

type 'a instance = Open of 'a | Closed

type 'a t = 'a instance ref

let v x = ref (Open x)

let get t = match !t with Open x -> `Open x | Closed -> `Closed

let get_exn t =
  match !t with Open instance -> instance | Closed -> raise Closed

let close t = t := Closed
