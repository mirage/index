open Common
open Cmdliner

let key_size = 32

let value_size = 13

let index_size = 10_000_000

let hash_size = 30

let index_name = Filename.concat "_bench" "hello"

let log_size = 500_000

let () = Random.init 1

module Context = Make_context (struct
  let key_size = key_size

  let hash_size = hash_size

  let value_size = value_size
end)

module Index = Index_unix.Make (Context.Key) (Context.Value)

let pp_stats ppf (count, max) =
  Fmt.pf ppf "\t%4dk/%dk" (count / 1000) (max / 1000)

let rec replaces t bindings i =
  if i = 0 then bindings
  else
    let count = index_size - i in
    if count mod 1_000 = 0 then Fmt.epr "\r%a%!" pp_stats (count, index_size);
    let k, v = (Context.Key.v (), Context.Value.v ()) in
    Index.replace t k v;
    replaces t ((k, v) :: bindings) (i - 1)

let rec finds t count = function
  | [] -> ()
  | (k, _) :: tl ->
      if count mod 1_000 = 0 then Fmt.epr "\r%a%!" pp_stats (count, index_size);
      ignore (Index.find t k);
      finds t (count + 1) tl

let run input =
  Fmt.epr "Adding %d bindings.\n%!" index_size;
  let rw = Index.v ~fresh:true ~log_size index_name in
  let bindings =
    match input with
    | `Write | `All ->
        let bindings, t1 = with_timer (fun () -> replaces rw [] index_size) in
        Fmt.epr "\n%d bindings added in %fs.\n%!" index_size t1;
        bindings
    | _ -> replaces rw [] index_size
  in
  Fmt.epr "Finding %d bindings.\n%!" index_size;
  let () =
    match input with
    | `Find `RW | `All ->
        let (), t2 = with_timer (fun () -> finds rw 0 bindings) in
        Fmt.epr "\n%d bindings found in %fs (RW).\n%!" index_size t2
    | _ -> ()
  in
  let () =
    match input with
    | `Find `RO| `All ->
        let ro = Index.v ~readonly:true ~log_size index_name in
        let (), t3 = with_timer (fun () -> finds ro 0 bindings) in
        Index.close ro;
        Fmt.epr "\n%d bindings found in %fs (RO).\n%!" index_size t3
    | _ -> ()
  in
  Index.close rw

let input =
  let doc =
    "Select which benchmark(s) to run. Available options are: `write`, \
     `find-rw`, `find-ro` or `all`."
  in
  let options =
    Arg.enum
      [
        ("all", `All);
        ("write", `Write);
        ("find-rw", `Find `RW);
        ("find-ro", `Find `RO);
      ]
  in
  Arg.(value & opt options `All & info [ "b"; "bench" ] ~doc)

let cmd =
  let doc = "Specify the benchmark you want to run." in
  (Term.(const run $ input), Term.info "run" ~doc ~exits:Term.default_exits)

let () = Term.(exit @@ eval cmd)
