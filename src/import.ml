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
