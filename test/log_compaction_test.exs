defmodule LogCompactionTest do
  use ExUnit.Case

  @tag only: true
  @tag compaction: true
  test "Log compaction deduplicates commands and config changes" do
    # Enable log compaction
    Application.put_env(:exra, :compact_logs, true)
    on_exit(fn -> Application.delete_env(:exra, :compact_logs) end)

    # Start 3 nodes
    {:ok, pid1} = Exra.start_link([name: :n1, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid2} = Exra.start_link([name: :n2, init_nodes: [], auto_tick: false, subscriber: self()])
    {:ok, pid3} = Exra.start_link([name: :n3, init_nodes: [], auto_tick: false, subscriber: self()])

    nodes = [pid1, pid2, pid3]
    nodes |> Enum.each(fn (node) ->
      node |> GenServer.cast({:init_set_nodes, nodes})
    end)

    # Elect pid1 as leader
    send(pid1, :timeout)
    assert_receive({:leader, ^pid1, _})
    assert_receive({:follower, ^pid2, _})
    assert_receive({:follower, ^pid3, _})

    # Append logs with the same key
    GenServer.cast(pid1, {:append_from_user, {:key1, "value1"}})
    assert_receive({:committed, _, _})

    GenServer.cast(pid1, {:append_from_user, {:key1, "value2"}})
    assert_receive({:committed, _, _})

    GenServer.cast(pid1, {:append_from_user, {:key2, "value3"}})
    assert_receive({:committed, _, _})

    GenServer.cast(pid1, {:append_from_user, {:key1, "value4"}})
    assert_receive({:committed, _, _})

    # Check state on leader
    state = GenServer.call(pid1, :get_state)
    # Expected logs:
    # - Genesis (index 0)
    # - {:key2, "value3"} (index 3)
    # - {:key1, "value4"} (index 4)
    # Note: indexes 1 and 2 ({key1, "value1"} and {key1, "value2"}) should be removed by compaction
    # But wait, index numbers are preserved in the log struct, they just won't be in the list?
    # Or does Raft rely on contiguous indexes?
    # The user asked to "Delete old committed logs".
    # The implementation removes them from the list.
    # So `logs` list should be shorter.

    logs = state.logs
    # Logs are stored new -> old
    # index 4: {:key1, "value4"}
    # index 3: {:key2, "value3"}
    # index 0: genesis
    # Deleted: index 2, index 1

    assert length(logs) == 3
    assert Enum.find(logs, fn log -> log.index == 4 end).command == {:key1, "value4"}
    assert Enum.find(logs, fn log -> log.index == 3 end).command == {:key2, "value3"}
    refute Enum.find(logs, fn log -> log.index == 2 end)
    refute Enum.find(logs, fn log -> log.index == 1 end)

    # Check follower state (should be replicated)
    state2 = GenServer.call(pid2, :get_state)
    logs2 = state2.logs
    assert length(logs2) == 3
    assert Enum.find(logs2, fn log -> log.index == 4 end).command == {:key1, "value4"}

    # Now test Config Change compaction
    # We already have config change logs from `init_set_nodes`? No, that's a cast, not a log entry.
    # Actually `init_set_nodes` just sets the state.
    # Let's verify Genesis log type.
    assert (logs |> List.last).type == :genesis

    # Trigger config changes
    # `new_nodes` call on leader triggers a config change log
    # Let's add a node
    {:ok, pid4} = Exra.start_link([name: :n4, init_nodes: [], auto_tick: false, subscriber: self(), state: :learner])
    GenServer.call(pid1, {:new_nodes, [pid1, pid2, pid3, pid4]})

    # Wait for commit of index 5 (Config Change Add)
    assert_receive({:committed, _, [%{new_committed_index: 5}]})

    # This should have added a config change log.
    _state = GenServer.call(pid1, :get_state)
    # New log added.

    # Trigger another config change (remove pid4)
    GenServer.call(pid1, {:new_nodes, [pid1, pid2, pid3]})
    # Wait for commit of index 6 (Config Change Remove)
    assert_receive({:committed, _, [%{new_committed_index: 6}]})

    # Now verify logs
    state = GenServer.call(pid1, :get_state)
    logs = state.logs

    # We expect:
    # - Latest config change (removing pid4)
    # - Key logs (compacted)
    # - Genesis
    # Previous config change (adding pid4) should be gone.

    config_changes = Enum.filter(logs, fn log -> log.type == :config_change end)
    assert length(config_changes) == 1

    latest_config = hd(config_changes)
    # The command for config change is {old_nodes, new_nodes}
    # Latest command was remove pid4: {[p1,p2,p3,p4], [p1,p2,p3]}
    {_old, new_nodes} = latest_config.command
    assert length(new_nodes) == 3
  end
end
