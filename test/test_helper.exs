require KVS
:kvs.join()
ExUnit.start()

defmodule BPE.Test do
  use ExUnit.Case, async: true


  defmodule KVS.Test do
    require KVS
    use ExUnit.Case, async: true

    test "basic" do
      id1 = {:basic, :kvs.seq([], [])}
      id2 = {:basic, :kvs.seq([], [])}
      x = 5
      :kvs.save(:kvs.writer(id1))
      :kvs.save(:kvs.writer(id2))

      Enum.each([id1, id2], fn id ->
                              writer = Enum.reduce(:lists.seq(1, x), :kvs.writer(id), fn id0, writer0 ->
                                                              :kvs.add(KVS.writer(writer0, args: {:"$msg", id0, [], [], [], []}))
                                                            end)
                             :kvs.save(writer)
      end)
      c = :kvs.feed(id1)
      c1 = :kvs.all(:"$msg")
      c0 = :lists.reverse(c)
      assert c0 == c1
      assert c != []

      r1 = :kvs.save(:kvs.reader(id1))
      r2 = :kvs.save(:kvs.reader(id2))
      x1 = :kvs.take(KVS.reader(:kvs.load_reader(KVS.reader(r1, :id)), args: 20))
      x2 = :kvs.take(KVS.reader(:kvs.load_reader(KVS.reader(r2, :id)), args: 20))

      b = :kvs.feed(id2)
      b1 = :kvs.all(:"$msg")
      b0 = :lists.reverse(b)

      assert b == KVS.reader(x2, :args)
      assert b != []

      #   _ ->
      #     # mnesia doesn't support `all` over feeds (only for tables)
      #     []
      # end

      assert KVS.reader(x1, :args) == b
      assert length(KVS.reader(x1, :args)) == length(KVS.reader(x2, :args))
      assert x == length(b)
    end



  test "sym" do
    id = {:sym, :kvs.seq([], [])}
    :kvs.save(:kvs.writer(id))
    x = 5

    :lists.map(
      fn
        z ->
          :kvs.remove(KVS.writer(z, :cache), id)
      end, :lists.map(
        fn _ ->
          :kvs.save(:kvs.add(KVS.writer(:kvs.writer(id), args: {:"$msg", [], [], [], [], []})))
        end,
        :lists.seq(1, x)
      )
    )

    {:ok, KVS.writer(count: 0)} = :kvs.get(:writer, id)
  end

  test "take" do
    id = {:partial, :kvs.seq([], [])}
    x = 5
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    t = :kvs.take(KVS.reader(:kvs.load_reader(KVS.reader(r, :id)), args: 20))
    b = :kvs.feed(id)
    # mnesia
    assert KVS.reader(t, :args) == b
  end

  test "partial take" do
    id = {:partial, :kvs.seq([], [])}
    x = 5
    :kvs.save(:kvs.writer(id))
    :lists.map(fn _ -> :kvs.append({:"$msg", [], [], [], [], []}, id) end, :lists.seq(1, x))
    r = :kvs.save(:kvs.reader(id))
    rid = KVS.reader(r, :id)
    p = 2

    cache = KVS.reader(r, :cache)
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    a = :lists.reverse(KVS.reader(t, :args))
    z1 = a
    r = :kvs.save(t)
    IO.inspect({cache, r, a})
    assert {:erlang.element(1, hd(a)), :erlang.element(2, hd(a))} == cache
    assert length(a) == p

    cache = KVS.reader(r, :cache)
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    a = :lists.reverse(KVS.reader(t, :args))
    r = :kvs.save(t)
    z2 = a
    IO.inspect({cache, r, a})
    assert {:erlang.element(1, hd(a)), :erlang.element(2, hd(a))} == cache
    assert length(a) == p

    cache = KVS.reader(r, :cache)
    t = :kvs.take(KVS.reader(:kvs.load_reader(rid), args: p))
    a = :lists.reverse(KVS.reader(t, :args))
    r = :kvs.save(t)
    z3 = a
    assert {:erlang.element(1, hd(a)), :erlang.element(2, hd(a))} == cache
    IO.inspect({cache, t, a})
    assert length(a) == 1

    assert :lists.reverse(z1 ++ z2 ++ z3) == :kvs.feed(id)
  end
end
