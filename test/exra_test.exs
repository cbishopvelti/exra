defmodule ExraTest do
  use ExUnit.Case
  doctest Exra

  # test "greets the world" do
  #   assert Exra.hello() == :world
  # end

  # @tag only: true
  test "3 nodes startup" do
    {:ok, pid1} = Exra.start_link(%{name: :n1, init_nodes: [], auto_tick: false, subscriber: self()})
    {:ok, pid2} = Exra.start_link(%{name: :n2, init_nodes: [], auto_tick: false, subscriber: self()})
    {:ok, pid3} = Exra.start_link(%{name: :n3, init_nodes: [], auto_tick: false, subscriber: self()})

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)

    send(pid1, :timeout)
    # ensure we've sent all messages before querying other nodes
    assert_receive({:leader, ^pid1})
    assert_receive({:follower, ^pid2})
    assert_receive({:follower, ^pid3})

    # Process.sleep(20)
    assert GenServer.call(pid3, :get_state).term == 1
    assert GenServer.call(pid3, :get_state).state == :follower
    assert GenServer.call(pid2, :get_state).term == 1
    assert GenServer.call(pid2, :get_state).state == :follower
    assert GenServer.call(pid1, :get_state).state == :leader
    assert GenServer.call(pid1, :get_state).term == 1
  end

  # @tag only: true
  test "leader split brain" do
    Mimic.set_mimic_global()

    {:ok, pid1} = Exra.start_link(%{name: :n1, init_nodes: [], auto_tick: false, subscriber: self()})
    {:ok, pid2} = Exra.start_link(%{name: :n2, init_nodes: [], auto_tick: false, subscriber: self()})
    {:ok, pid3} = Exra.start_link(%{name: :n3, init_nodes: [], auto_tick: false, subscriber: self()})

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)

    send(pid1, :timeout)
    assert_receive({:leader, ^pid1})
    assert_receive({:follower, ^pid2})
    assert_receive({:follower, ^pid3})

    assert GenServer.call(pid1, :get_state).state == :leader
    assert GenServer.call(pid1, :get_state).term == 1

    Mimic.stub(Exra, :broadcast_from, fn
      (message, state = %{name: :n1}) ->
        state.nodes
        |> Enum.filter(fn (node) -> node != self() end )
        |> Enum.filter(fn (_) -> false end) # we're disconected, can't send to anyone
        |> Enum.each(fn (node) ->
          GenServer.cast(node, message)
        end)
      (message, state) ->
        state.nodes
        |> Enum.filter(fn (node) -> node != self() end )
        |> Enum.filter(fn (node) -> node != pid1 end) # Can't send to n1, it's disconnected
        |> Enum.each(fn (node) ->
          GenServer.cast(node, message)
        end)
    end)

    send(pid2, :timeout)
    assert_receive({:leader, ^pid2})
    assert_receive({:follower, ^pid3})

    GenServer.call(pid2, :get_state)
    GenServer.call(pid3, :get_state)

    assert GenServer.call(pid2, :get_state).state == :leader
    assert GenServer.call(pid2, :get_state).term == 2

    send(pid1, :send_ping)
    assert GenServer.call(pid1, :get_state).term == 1
    assert GenServer.call(pid1, :get_state).state == :leader
  end

  # @tag only: true
  test "Log replication" do
    Mimic.set_mimic_global()
    {:ok, pid1} = Exra.start_link(%{name: :n1, init_nodes: [], auto_tick: false, subscriber: self()})
    {:ok, pid2} = Exra.start_link(%{name: :n2, init_nodes: [], auto_tick: false, subscriber: self()})
    {:ok, pid3} = Exra.start_link(%{name: :n3, init_nodes: [], auto_tick: false, subscriber: self()})

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)

    send(pid1, :timeout)
    assert_receive({:leader, ^pid1})
    assert_receive({:follower, ^pid2})
    assert_receive({:follower, ^pid3})

    GenServer.cast(pid1, {:append_from_user, "a log entry"})
    assert_receive({:committed, %{
      pid: ^pid1,
      state: :leader,
      new_committed_index: 1,
      old_committed_index: 0
    }})
    assert_receive({:committed, %{
      pid: ^pid2,
      state: :follower,
      new_committed_index: 1,
      old_committed_index: 0
    }})
    assert_receive({:committed, %{
      pid: ^pid3,
      state: :follower,
      new_committed_index: 1,
      old_committed_index: 0
    }})

    assert GenServer.call(pid1, :get_state).committed_index  == 1
    assert GenServer.call(pid1, :get_state).logs |> length() == 2

    assert GenServer.call(pid2, :get_state).committed_index  == 1
    assert GenServer.call(pid2, :get_state).logs |> length() == 2
  end

  @tag only: true
  test "A down follower should catch up" do
    Mimic.set_mimic_global()
    {:ok, pid1} = Exra.start_link(%{name: :n1, init_nodes: [], auto_tick: false, subscriber: self()})
    {:ok, pid2} = Exra.start_link(%{name: :n2, init_nodes: [], auto_tick: false, subscriber: self()})
    {:ok, pid3} = Exra.start_link(%{name: :n3, init_nodes: [], auto_tick: false, subscriber: self()})

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)

    send(pid1, :timeout)
    assert_receive({:leader, ^pid1})
    assert_receive({:follower, ^pid2})
    assert_receive({:follower, ^pid3})


    GenServer.cast(pid1, {:append_from_user, "a log entry"})
    assert_receive({:committed, %{
      pid: ^pid1,
      state: :leader,
      new_committed_index: 1,
      old_committed_index: 0
    }})
    assert_receive({:committed, %{
      pid: ^pid2,
      state: :follower,
      new_committed_index: 1,
      old_committed_index: 0
    }})
    assert_receive({:committed, %{
      pid: ^pid3,
      state: :follower,
      new_committed_index: 1,
      old_committed_index: 0
    }})

    Mimic.stub(Exra, :handle_cast, fn (message, state) ->
      case self() do
        ^pid3 -> {:noreply, state} # Does nothing
        _ -> Mimic.call_original(Exra, :handle_cast, [message, state])
      end
    end)

    GenServer.cast(pid1, {:append_from_user, "second log entry"})
    assert_receive({:committed, %{
      pid: ^pid1,
      state: :leader,
      new_committed_index: 2,
      old_committed_index: 1
    }})
    assert_receive({:committed, %{
      pid: ^pid2,
      state: :follower,
      new_committed_index: 2,
      old_committed_index: 1
    }})

    # GenServer.call(pid1, :get_state) |> IO.inspect(label: "002")
    assert GenServer.call(pid1, :get_state).committed_index == 2
    assert GenServer.call(pid1, :get_state).logs |> length == 3
    assert GenServer.call(pid2, :get_state).committed_index == 2
    assert GenServer.call(pid2, :get_state).logs |> length == 3
    assert GenServer.call(pid3, :get_state).committed_index == 1
    assert GenServer.call(pid3, :get_state).logs |> length == 2


    Mimic.stub(Exra, :handle_cast, fn (msg, state) ->
      Mimic.call_original(Exra, :handle_cast, [msg, state])
    end)

    GenServer.cast(pid1, {:append_from_user, "Third log entry"})
    GenServer.call(pid1, :get_state)
    GenServer.call(pid3, :get_state)

    assert_receive({:committed, %{
      pid: ^pid1,
      state: :leader,
      new_committed_index: 3,
      old_committed_index: 2
    }})
    assert_receive({:committed, %{
      pid: ^pid2,
      state: :follower,
      new_committed_index: 3,
      old_committed_index: 2
    }})
    assert_receive({:committed, %{
      pid: ^pid3,
      state: :follower,
      new_committed_index: 3,
      old_committed_index: 1
    }})

    assert GenServer.call(pid3, :get_state).committed_index == 3
    assert GenServer.call(pid3, :get_state).logs |> length == 4
  end
end
