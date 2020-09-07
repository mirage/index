include Platform_intf

module Memory = struct
  module Lock () = struct
    type t = Mutex.t

    let pool = Hashtbl.create 0

    let pool_lock = Mutex.create ()

    let lock name =
      Mutex.lock pool_lock;
      match Hashtbl.find_opt pool name with
      | Some m ->
          Mutex.unlock pool_lock;
          Mutex.lock m;
          m
      | None ->
          let m = Mutex.create () in
          Hashtbl.add pool name m;
          Mutex.unlock pool_lock;
          Mutex.lock m;
          m

    let unlock = Mutex.unlock
  end

  module IO () = struct
    type instance = {
      mutable name : string;
      mutable generation : int64;
      mutable fan : string;
      mutable fan_size : int64;
      mutable data : Buffer.t;
      readonly : bool;
    }

    type t = instance Closeable.t

    let pool = Hashtbl.create 0

    let v ?flush_callback:_ ~readonly ~fresh:_ ~generation ~fan_size name =
      let new_instance () =
        Closeable.v
          {
            name;
            readonly;
            generation;
            fan_size;
            fan = String.init (Int64.to_int fan_size) (fun _ -> Char.chr 0);
            data = Buffer.create 0;
          }
      in
      match Hashtbl.find_opt pool (name, readonly) with
      | Some i -> i
      | None ->
          let i = new_instance () in
          Hashtbl.add pool (name, readonly) i;
          i

    let clear ~generation t =
      let t = Closeable.get_exn t in
      Buffer.clear t.data;
      t.generation <- generation;
      ()

    let close t_handle =
      Log.debug (fun m -> m "Close");
      let t = Closeable.get_exn t_handle in
      Buffer.clear t.data;
      Closeable.close t_handle;
      Hashtbl.remove pool (t.name, t.readonly)

    let append t s =
      let t = Closeable.get_exn t in
      Buffer.add_string t.data s

    let rename ~src:src_handle ~dst =
      let src = Closeable.get_exn src_handle in
      let dst = Closeable.get_exn dst in
      dst.name <- src.name;
      dst.generation <- src.generation;
      dst.fan <- src.fan;
      dst.fan_size <- src.fan_size;
      dst.data <- src.data;
      (* Closeable.close src_handle; *)
      ()

    let get_fanout t =
      let t = Closeable.get_exn t in
      t.fan

    let set_fanout t fan =
      let t = Closeable.get_exn t in
      Format.eprintf "%d\n" (Buffer.length t.data);
      Format.eprintf "%Ld\n" t.fan_size;

      assert (Int64.equal (Int64.of_int (Buffer.length t.data)) t.fan_size);
      t.fan <- fan

    let flush ?no_callback:_ ?with_fsync:_ t =
      let _ = Closeable.get_exn t in
      ()

    let read t ~off ~len b =
      let t = Closeable.get_exn t in
      let off = Int64.to_int off in
      Buffer.blit t.data off b len 0;
      len

    let offset_instance t = Buffer.length t.data |> Int64.of_int

    let offset t =
      let t = Closeable.get_exn t in
      offset_instance t

    module Header = struct
      let set t { offset = _; generation } =
        let t = Closeable.get_exn t in
        (* TODO: offset *)
        t.generation <- generation

      let get t =
        let t = Closeable.get_exn t in
        { offset = offset_instance t; generation = t.generation }

      let get_generation t =
        let t = Closeable.get_exn t in
        t.generation
    end
  end
end
