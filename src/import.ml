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

  let t : t Repr.t =
    let open Repr in
    (map int64) of_int64 to_int64
    |> like ~pp:Optint.Int63.pp ~equal:(stage Optint.Int63.equal)
         ~compare:(stage Optint.Int63.compare)
end

type int63 = Int63.t [@@deriving repr]
