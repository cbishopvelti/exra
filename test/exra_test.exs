defmodule ExraTest do
  use ExUnit.Case
  doctest Exra

  # test "greets the world" do
  #   assert Exra.hello() == :world
  # end

  @tag leader: true
  test "3 nodes startup" do
    {:ok, pid1} = Exra.start_link([name: :n1, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid2} = Exra.start_link([name: :n2, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid3} = Exra.start_link([name: :n3, init_nodes: [], auto_tick: false, subscriber: self()])

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)

    send(pid1, :timeout)
    # ensure we've sent all messages before querying other nodes
    assert_receive({:leader, ^pid1, _})
    assert_receive({:follower, ^pid2, _})
    assert_receive({:follower, ^pid3, _})

    # Process.sleep(20)
    assert GenServer.call(pid3, :get_state).term == 1
    assert GenServer.call(pid3, :get_state).state == :follower
    assert GenServer.call(pid2, :get_state).term == 1
    assert GenServer.call(pid2, :get_state).state == :follower
    assert GenServer.call(pid1, :get_state).state == :leader
    assert GenServer.call(pid1, :get_state).term == 1
  end

  @tag leader: true
  test "leader split brain" do
    Mimic.set_mimic_global()

    {:ok, pid1} = Exra.start_link([name: :n1, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid2} = Exra.start_link([name: :n2, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid3} = Exra.start_link([name: :n3, init_nodes: [], auto_tick: false, subscriber: self()])

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)

    send(pid1, :timeout)
    assert_receive({:leader, ^pid1, _})
    assert_receive({:follower, ^pid2, _})
    assert_receive({:follower, ^pid3, _})

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
    assert_receive({:leader, ^pid2, _})
    # assert_receive({:follower, ^pid3, _}) # We where already a leader, so this wont be called

    GenServer.call(pid2, :get_state)
    GenServer.call(pid3, :get_state)

    assert GenServer.call(pid2, :get_state).state == :leader
    assert GenServer.call(pid2, :get_state).term == 2

    send(pid1, :send_heartbeat)
    assert GenServer.call(pid1, :get_state).term == 1
    assert GenServer.call(pid1, :get_state).state == :leader
  end

  # @tag leader: true
  # test "Single node should instantly become leader" do
  #   {:ok, pid1} = Exra.start_link([name: :n1, init_nodes: [], auto_tick: false, subscriber: self()])

  #   GenServer.call(pid1)
  # end

  @tag logs: true
  test "Log replication" do
    Mimic.set_mimic_global()
    {:ok, pid1} = Exra.start_link([name: :n1, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid2} = Exra.start_link([name: :n2, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid3} = Exra.start_link([name: :n3, init_nodes: [], auto_tick: false, subscriber: self()])

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)

    send(pid1, :timeout)
    assert_receive({:leader, ^pid1, _})
    assert_receive({:follower, ^pid2, _})
    assert_receive({:follower, ^pid3, _})

    GenServer.cast(pid1, {:append_from_user, "a log entry"})
    assert_receive({:committed, _, [%{
      pid: ^pid1,
      state: :leader,
      new_committed_index: 1,
      old_committed_index: 0
    }]})
    assert_receive({:committed, _, [%{
      pid: ^pid2,
      state: :follower,
      new_committed_index: 1,
      old_committed_index: 0
    }]})
    assert_receive({:committed, _, [%{
      pid: ^pid3,
      state: :follower,
      new_committed_index: 1,
      old_committed_index: 0
    }]})

    assert GenServer.call(pid1, :get_state).committed_index  == 1
    assert GenServer.call(pid1, :get_state).logs |> length() == 2

    assert GenServer.call(pid2, :get_state).committed_index  == 1
    assert GenServer.call(pid2, :get_state).logs |> length() == 2
  end

  @tag logs: true
  test "A down follower should catch up" do
    Mimic.set_mimic_global()
    {:ok, pid1} = Exra.start_link([name: :n1, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid2} = Exra.start_link([name: :n2, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid3} = Exra.start_link([name: :n3, init_nodes: [], auto_tick: false, subscriber: self()])

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)

    send(pid1, :timeout)
    assert_receive({:leader, ^pid1, _})
    assert_receive({:follower, ^pid2, _})
    assert_receive({:follower, ^pid3, _})


    GenServer.cast(pid1, {:append_from_user, "a log entry"})
    assert_receive({:committed, _, [%{
      pid: ^pid1,
      state: :leader,
      new_committed_index: 1,
      old_committed_index: 0
    }]})
    assert_receive({:committed, _, [%{
      pid: ^pid2,
      state: :follower,
      new_committed_index: 1,
      old_committed_index: 0
    }]})
    assert_receive({:committed, _, [%{
      pid: ^pid3,
      state: :follower,
      new_committed_index: 1,
      old_committed_index: 0
    }]})

    Mimic.stub(Exra, :handle_cast, fn (message, state) ->
      case self() do
        ^pid3 -> {:noreply, state} # Does nothing
        _ -> Mimic.call_original(Exra, :handle_cast, [message, state])
      end
    end)

    GenServer.cast(pid1, {:append_from_user, "second log entry"})
    assert_receive({:committed, _, [%{
      pid: ^pid1,
      state: :leader,
      new_committed_index: 2,
      old_committed_index: 1
    }]})
    assert_receive({:committed, _, [%{
      pid: ^pid2,
      state: :follower,
      new_committed_index: 2,
      old_committed_index: 1
    }]})

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

    assert_receive({:committed, _, [%{
      pid: ^pid1,
      state: :leader,
      new_committed_index: 3,
      old_committed_index: 2
    }]})
    assert_receive({:committed, _, [%{
      pid: ^pid2,
      state: :follower,
      new_committed_index: 3,
      old_committed_index: 2
    }]})
    # Two, as it'll broadcast on when it's cought up, and then again on new message
    assert_receive({:committed, _, [%{
      pid: ^pid3,
      state: :follower,
      new_committed_index: 2,
      old_committed_index: 1
    }]})
    assert_receive({:committed, _, [%{
      pid: ^pid3,
      state: :follower,
      new_committed_index: 3,
      old_committed_index: 2
    }]})

    assert GenServer.call(pid3, :get_state).committed_index == 3
    assert GenServer.call(pid3, :get_state).logs |> length == 4
  end

  @tag config_change: true
  test "Add one node" do
    {:ok, pid1} = Exra.start_link([name: :n1, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid2} = Exra.start_link([name: :n2, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid3} = Exra.start_link([name: :n3, init_nodes: [], auto_tick: false, subscriber: self()])

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)
    send(pid1, :timeout)
    assert_receive({:leader, ^pid1, _})
    assert_receive({:follower, ^pid2, _})
    assert_receive({:follower, ^pid3, _})

    {:ok, pid4} = Exra.start_link([name: :n4, init_nodes: [], auto_tick: false, subscriber: self(), state: :learner])
    GenServer.call(pid1, {:new_nodes, [pid1, pid2, pid3, pid4]})
    assert_receive{:committed, _, [%{
      pid: ^pid4,
      state: :learner
    }]}

    assert_receive({:committed, _, [%{
      pid: ^pid1,
      state: :leader,
      old_committed_index: 0,
      new_committed_index: 1
    }]})

    GenServer.call(pid1, :get_state)
    assert GenServer.call(pid1, :get_state).nodes |> length == 3
    assert GenServer.call(pid1, :get_state).logs |> length == 2
  end
  @tag config_change: true
  test "Add one node with log entry" do
    {:ok, pid1} = Exra.start_link([name: :n1, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid2} = Exra.start_link([name: :n2, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid3} = Exra.start_link([name: :n3, init_nodes: [], auto_tick: false, subscriber: self()])

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)
    send(pid1, :timeout)
    assert_receive({:leader, ^pid1, _})
    assert_receive({:follower, ^pid2, _})
    assert_receive({:follower, ^pid3, _})

    GenServer.cast(pid1, {:append_from_user, "First log entry"})
    assert_receive({:committed, _, [%{
      pid: ^pid1,
      state: :leader,
      new_committed_index: 1,
      old_committed_index: 0
    }]})
    assert_receive({:committed, _, [%{
      pid: ^pid2,
      state: :follower,
      new_committed_index: 1,
      old_committed_index: 0
    }]})
    assert_receive({:committed, _, [%{
      pid: ^pid3,
      state: :follower,
      new_committed_index: 1,
      old_committed_index: 0
    }]})

    # IO.puts("-------------------------------")

    {:ok, pid4} = Exra.start_link([name: :n4, init_nodes: [], auto_tick: false, subscriber: self(), state: :learner])
    GenServer.call(pid1, {:new_nodes, [pid1, pid2, pid3, pid4]})
    assert_receive{:committed, _, [%{
      pid: ^pid4,
      state: :learner,
      new_committed_index: 1,
      old_committed_index: 0
    }]}
    assert_receive{:committed, _, [%{
      pid: ^pid4,
      state: :follower,
      new_committed_index: 2,
      old_committed_index: 1
    }]}

    assert_receive({:committed, _, [%{
      pid: ^pid1,
      state: :leader,
      old_committed_index: 1,
      new_committed_index: 2
    }]})

    GenServer.call(pid1, :get_state)
    assert GenServer.call(pid1, :get_state).nodes |> length == 3
    assert GenServer.call(pid1, :get_state).logs |> length == 3
  end

  @tag config_change: true
  test "remove one node inc log entry" do
    {:ok, pid1} = Exra.start_link([name: :n1, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid2} = Exra.start_link([name: :n2, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid3} = Exra.start_link([name: :n3, init_nodes: [], auto_tick: false, subscriber: self()])

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)
    send(pid1, :timeout)
    assert_receive({:leader, ^pid1, _})
    assert_receive({:follower, ^pid2, _})
    assert_receive({:follower, ^pid3, _})

    GenServer.cast(pid1, {:append_from_user, "First log entry"})
    assert_receive({:committed, _, [%{pid: ^pid1}]})
    assert_receive({:committed, _, [%{pid: ^pid2}]})
    assert_receive({:committed, _, [%{pid: ^pid3}]})

    GenServer.call(pid1, {:new_nodes, [pid1, pid2]})

    assert_receive({:removed, ^pid3, _})

    assert GenServer.call(pid1, :get_state).nodes |> MapSet.new() |> MapSet.equal?(MapSet.new([pid2]))
    assert GenServer.call(pid2, :get_state).nodes |> MapSet.new() |> MapSet.equal?(MapSet.new([pid1]))
  end

  @tag config_change: true
  test "Remove the current leader" do
    {:ok, pid1} = Exra.start_link([name: :n1, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid2} = Exra.start_link([name: :n2, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid3} = Exra.start_link([name: :n3, init_nodes: [], auto_tick: false, subscriber: self()])

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)
    send(pid1, :timeout)
    assert_receive({:leader, ^pid1, _})
    assert_receive({:follower, ^pid2, _})
    assert_receive({:follower, ^pid3, _})

    GenServer.call(pid1, {:new_nodes, [pid2, pid3]})
    assert_receive({:removed, ^pid1, [^pid1]})

    assert GenServer.call(pid2, :get_state).nodes |> length == 1
    assert GenServer.call(pid3, :get_state).nodes |> length == 1
  end
end
