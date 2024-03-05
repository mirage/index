module Make (Platform : Common.Platform) : sig
  val tests : io:Platform.io -> unit Alcotest.test_case list
end
