module Make (K : Deudex.Key) (V : Deudex.Value) :
  Deudex.S with type key = K.t and type value = V.t
