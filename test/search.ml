module Entry = struct
  module Key = struct
    type t = int

    let equal = ( = )
  end

  module Value = struct
    type t = string
  end

  type t = Key.t * Value.t

  let to_key = fst

  let to_value = snd
end

module EltArray = struct
  type t = Entry.t array

  type elt = Entry.t

  let get t i = t.(Int64.to_int i)

  let length t = Array.length t |> Int64.of_int

  let pre_fetch _ ~low:_ ~high:_ = ()
end

(* Metric equal to an integer key *)
module Metric_key = struct
  module Entry = Entry

  type t = Entry.Key.t

  let compare : t -> t -> int = compare

  let of_entry = Entry.to_key

  let of_key k = k

  let linear_interpolate ~low:(low_out, low_in) ~high:(high_out, high_in) m =
    let low_in = float_of_int low_in in
    let high_in = float_of_int high_in in
    let target_in = float_of_int m in
    let low_out = Int64.to_float low_out in
    let high_out = Int64.to_float high_out in
    (* Fractional position of [target_in] along the line from [low_in] to [high_in] *)
    let proportion = (target_in -. low_in) /. (high_in -. low_in) in
    (* Convert fractional position to position in output space *)
    let position = low_out +. (proportion *. (high_out -. low_out)) in
    let rounded = ceil (position -. 0.5) +. 0.5 in
    Int64.of_float rounded
end

module Search = Index.Private.Search.Make (Entry) (EltArray) (Metric_key)

let interpolation_unique () =
  let array = Array.init 10_000 (fun i -> (i, string_of_int i)) in
  let length = EltArray.length array in
  Array.iter
    (fun (i, v) ->
      Search.interpolation_search array i
        ~low:Int64.(zero)
        ~high:Int64.(pred length)
      |> Alcotest.(check string) "" v)
    array

(* Degenerate metric that is the same for all entries *)
module Metric_constant = struct
  module Entry = Entry

  type t = unit

  let compare () () = 0

  let of_entry _ = ()

  let of_key _ = ()

  let linear_interpolate ~low:(low_out, _) ~high:(high_out, _) _ =
    let ( + ), ( - ) = Int64.(add, sub) in
    (* Any value in the range [low_out, high_out] is valid *)
    low_out + Random.int64 (high_out - low_out) + 1L
end

module Search_constant =
  Index.Private.Search.Make (Entry) (EltArray) (Metric_constant)

let interpolation_constant_metric () =
  let array = Array.init 100 (fun i -> (i, string_of_int i)) in
  let length = EltArray.length array in
  Array.iter
    (fun (i, v) ->
      Search_constant.interpolation_search array i ~low:0L
        ~high:Int64.(pred length)
      |> Alcotest.(check string) "" v)
    array

let () =
  Random.self_init ();
  Alcotest.run "search"
    [
      ( "interpolation",
        [
          Alcotest.test_case "unique" `Quick interpolation_unique;
          Alcotest.test_case "constant metric" `Quick
            interpolation_constant_metric;
        ] );
    ]
