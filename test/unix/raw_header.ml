module Int63 = Optint.Int63
module Raw = Index_unix.Private.Raw

(* [with_file contents f] calls [f] on a [Raw.t] for a fresh file that holds
   [contents]. *)
let with_file contents f =
  let path = Filename.temp_file "index-raw" "" in
  let fd = Unix.openfile path Unix.[ O_RDWR; O_CLOEXEC ] 0o600 in
  Fun.protect
    ~finally:(fun () ->
      Unix.close fd;
      Sys.remove path)
    (fun () ->
      let raw = Raw.v fd in
      if contents <> "" then
        Raw.unsafe_write raw ~off:Int63.zero contents 0 (String.length contents);
      f raw)

let check_not_written name f =
  Alcotest.check_raises name Raw.Not_written (fun () -> ignore (f ()))

(** Reading the header of a file that is empty, or that ends in the middle of
    the field to read, raises [Not_written]. *)
let truncated_header () =
  with_file "" (fun raw ->
      check_not_written "empty file, Header.get" (fun () -> Raw.Header.get raw);
      check_not_written "empty file, Offset.get" (fun () -> Raw.Offset.get raw));
  with_file (String.make 10 '\000') (fun raw ->
      check_not_written "10-byte file, Header.get" (fun () ->
          Raw.Header.get raw);
      check_not_written "10-byte file, Version.get" (fun () ->
          Raw.Version.get raw);
      check_not_written "10-byte file, Header_prefix.get" (fun () ->
          Raw.Header_prefix.get raw);
      Alcotest.(check int)
        "10-byte file, the offset is complete" 0
        (Int63.to_int (Raw.Offset.get raw)));
  with_file (String.make 28 '\000') (fun raw ->
      check_not_written "28-byte file, Fan.get_size" (fun () ->
          Raw.Fan.get_size raw))

(** The fan is checked against its recorded length, and an empty fan reads fine. *)
let fan () =
  with_file (String.make 32 '\000') (fun raw ->
      Alcotest.(check string) "empty fan" "" (Raw.Fan.get raw);
      Raw.Fan.set_size raw (Int63.of_int 16);
      check_not_written "fan shorter than its length" (fun () ->
          Raw.Fan.get raw);
      Raw.Fan.set raw (String.make 16 'f');
      Alcotest.(check string)
        "complete fan" (String.make 16 'f') (Raw.Fan.get raw))

(** Writing a version that is not 8 bytes long raises [Invalid_argument]. *)
let version_length () =
  with_file (String.make 32 '\000') (fun raw ->
      let invalid name f =
        match f () with
        | () -> Alcotest.failf "%s: Invalid_argument expected" name
        | exception Invalid_argument _ -> ()
      in
      invalid "Version.set" (fun () -> Raw.Version.set raw "v1");
      invalid "Header.set" (fun () ->
          Raw.Header.set raw
            { offset = Int63.zero; version = "v1"; generation = Int63.zero });
      invalid "Header_prefix.set" (fun () ->
          Raw.Header_prefix.set raw
            { offset = Int63.zero; version = "000000001" });
      Raw.Version.set raw "00000001";
      Alcotest.(check string) "version" "00000001" (Raw.Version.get raw))

let tests =
  [
    ("truncated header", `Quick, truncated_header);
    ("fan", `Quick, fan);
    ("version length", `Quick, version_length);
  ]
