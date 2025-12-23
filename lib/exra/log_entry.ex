defmodule Exra.LogEntry do
  require Logger
  require Exra
  @type t :: %__MODULE__{
    index: integer(),
    term: integer(),
    command: any(),
    type: :command | :config_change | :genesis
  }

  defstruct [
    :index,
    :term,
    :command,
    type: :command
  ]

  def handle_cast({:append_from_user, command}, state = %{
    state: :leader,
    term: term,
    logs: logs,
  }) do

    log = %{
      term: term,
      index: hd(logs).index + 1,
      command: command,
      type: :command
    }
    append_log(log, state)
  end
  # Forward messages to the leader
  def handle_cast(message = {:append_from_user, _}, state = %{
    leader: leader
  }) when not is_nil(leader) do
    GenServer.cast(leader, message)
    {:noreply, state}
  end

  def handle_cast({:config_change, command}, state = %{
    state: :leader,
    term: term,
    logs: logs
  }) do
    Logger.debug("LogEntry :config_change")
    log = %{
      term: term,
      index: hd(logs).index + 1,
      command: command,
      type: :config_change
    }

    state = %{state |
      nodes: MapSet.union(
        state.nodes |> MapSet.new(),
        command |> elem(1) |> MapSet.new()) |> MapSet.to_list() |> Enum.reject(fn (node) -> Exra.Utils.is_self?(node) end)
    }
    append_log(log, state)
  end

  # We're a follower, so we don't have the authority to do anything
  # def handle_cast({:replicated, _, _}, state = %{state: :follower}) do
  #   {:noreply, state}
  # end
  def handle_cast({:replicated, false, from}, state = %{state: :leader, next_indexes: next_indexes, logs: logs, term: term, committed_index: committed_index}) do
    # step back and send previous logs
    ni = (next_indexes |> Map.get(from, 1)) - 1
    new_next_indexes = next_indexes |> Map.put(from, ni)

    node = from
    logs_to_send = Enum.take_while(logs, fn (%{index: index}) -> index >= ni end)
    previous_log = Enum.find(logs, fn %{index: index} -> index == (max(1, ni) - 1) end)

    GenServer.cast(node, {:replicate, logs_to_send, %{
      index: previous_log.index,
      term: previous_log.term
    }, self(), term, committed_index})

    {:noreply, %{state | next_indexes: new_next_indexes }}
  end
  def handle_cast({:replicated, index, from}, state = %{
    state: :leader, next_indexes: next_indexes, logs: logs = [log | _],
    match_indexes: match_indexes, committed_index: committed_index, nodes: nodes,
    term: current_term
  }) do
    # IO.puts("004 :replicated #{index} #{inspect(from)}")
    new_next_indexes = next_indexes |> Map.put(from, index + 1)
    new_match_indexes = match_indexes |> Map.put(from, index)

    { c_old, c_new, c_log} = get_config_nodes(
      nodes,
      logs |> Enum.take_while(fn (%{index: index}) -> index > committed_index end)
    )
    c_old_new_match_indexes = Map.take(new_match_indexes, c_old)
    c_new_new_match_indexes = Map.take(new_match_indexes, c_new)

    c_old_commit = [log.index | (c_old_new_match_indexes |> Map.values())] |> Exra.Utils.median()
    c_new_commit = [log.index | (c_new_new_match_indexes |> Map.values())] |> Exra.Utils.median()

    calculated_index = min(c_old_commit, c_new_commit)
    # 4. SAFETY CHECK: Only update commit index if:
    #    a. It is larger than current committed_index
    #    b. The log entry at that index belongs to the CURRENT TERM
    #       (Raft Paper 5.4.2)
    target_log = Enum.find(logs, fn l -> l.index == calculated_index end)

    new_committed_index = if calculated_index > committed_index and target_log != nil and target_log.term == current_term do
      calculated_index
    else
      committed_index
    end

    nodes = if (!is_nil(c_log) and c_log.index <= new_committed_index) do # Config change is fully committed, so old nodes are no longer part of the cluster
      nodes_removed(MapSet.difference(
        [self() | nodes] |> Enum.uniq() |> MapSet.new(),
        c_new |> Enum.map(fn (node) ->
          case Exra.Utils.is_self?(node) do
            true -> self() # convert {Exra, a@127.0.0.1} to pid
            false -> node
          end
        end) |> MapSet.new()
      ) |> MapSet.to_list())
      nodes_added(
        MapSet.difference(
          c_new |> Enum.map(fn (node) ->
            case Exra.Utils.is_self?(node) do
              true -> self() # convert {Exra, a@127.0.0.1} to pid
              false -> node
            end
          end) |> MapSet.new(),
          [self() | nodes] |> Enum.uniq() |> MapSet.new()
        )
      )
      c_new
    else
      nodes
    end

    logs = case new_committed_index > committed_index do
      true ->

        nodes
        |> Enum.filter(fn (node) -> node != self() end)
        |> Enum.each(fn (node) ->
          GenServer.cast(node, {:committed_index, new_committed_index})
        end)
        Exra.Utils.notify_state_machine(state, :committed, [%{
          pid: self(),
          new_committed_index: new_committed_index,
          old_committed_index: committed_index,
          state: state.state
        }])
        compact_logs(logs, new_committed_index)
      false -> # Nothings changed, don't tell anyone
        logs
    end

    {:noreply, %{state | next_indexes: new_next_indexes, match_indexes: new_match_indexes, committed_index: new_committed_index,
      nodes:  nodes |> Enum.filter(fn (node) -> node != self() end),
      logs: logs
    }}
  end

  def handle_cast({:replicated, _index, _from}, state) do
    Logger.warning(":replicated, recieved message when not leader")
    {:noreply, state}
  end

  # Replicate/heartbeat
  def handle_cast({:replicate, _logs, _previous_log, from, term, _committed_index}, state = %{state: :follower, logs: _my_logs, term: my_term}) when term < my_term do
    GenServer.cast(from, {:stale, my_term})
    {:noreply, state}
  end
  def handle_cast({:replicate, logs, previous_log, from, term, committed_index}, state = %{state: my_state, logs: my_logs, term: my_term,
    timeout: timeout, committed_index: my_committed_index})
  do
    # IO.puts("002.2 replicate #{inspect(logs)}, #{committed_index}")
    !is_nil(timeout) && Process.cancel_timer(timeout)
    timeout = Exra.tick(:timeout, state)

    # state = if term >= state.term and (state.state != :follower or state.term == 0) do
    state = if term >= state.term do
        new_state = %{state | state: :follower, term: term, voted_for: (if term > state.term, do: nil, else: state.voted_for)}
        if (my_term == 0 or state.state != :follower) do
          Exra.Utils.notify_state_machine(state, :follower, [term, length(state.nodes) + 1])
        end
        new_state
    else
        state
    end

    previous_index = previous_log.index

    my_previous_log = my_logs |> Enum.find(fn (log) -> log.index == previous_index end)

    new_logs = case !is_nil(my_previous_log) && previous_log.term == my_previous_log.term do
      true -> # all is fine, commit
        out = logs ++ Enum.drop_while(my_logs, fn (log) -> log.index > previous_index end)

        GenServer.cast(from, {:replicated, hd(out).index, self()})

        if committed_index > my_committed_index or my_state == :learner  do
          Exra.Utils.notify_state_machine(state, :committed, [%{
            pid: self(),
            new_committed_index: committed_index,
            old_committed_index: my_committed_index,
            state: my_state
          }])
        end

        out
      false ->
        # Tell the leader we don't match and to step back the next_index
        GenServer.cast(from, {:replicated, false, self()})
        my_logs # my old logs, nothings changed
    end

    # Ensure we have all the nodes
    {c_old, c_new, _c_log} = get_config_nodes_v2(state.nodes,
      committed_index,
      state.logs
    )

    new_logs = compact_logs(new_logs, committed_index)

    {:noreply, %{state |
      logs: new_logs,
      state: :follower,
      term: term,
      voted_for: (if term > my_term, do: nil, else: state.voted_for),
      timeout: timeout,
      committed_index: committed_index,
      nodes: (c_new ++ c_old) |> Enum.reject(fn (node) -> Exra.Utils.is_self?(node)  end) |> Enum.uniq(),
      leader: from
    }}
  end

  def handle_cast(_message = {:committed_index, committed_index}, state = %{logs: logs = [log | _], committed_index: old_committed_index}) do
    new_committed_index = min(committed_index, log.index)
    if (new_committed_index != old_committed_index) do
      Exra.Utils.notify_state_machine(state, :committed, [%{
        pid: self(),
        new_committed_index: new_committed_index,
        old_committed_index: old_committed_index,
        state: state.state
      }])
    else
    end

    {old_nodes, new_nodes, _log} = get_config_nodes_v2(state.nodes, new_committed_index, logs)

    logs = compact_logs(logs, new_committed_index)

    {:noreply,
      %{state |
        committed_index: new_committed_index,
        nodes: (old_nodes ++ new_nodes) |> Enum.reject(fn (node) -> Exra.Utils.is_self?(node) end),
        logs: logs
      }
    }
  end

  def handle_call(message = {:config_change, _}, _from, state = %{state: :leader, nodes: nodes, logs: logs, committed_index: committed_index}) do
    {_old, _new, log} = get_config_nodes(nodes, logs |> Enum.take_while(fn (%{index: index}) -> index > committed_index end))
    case log do
      nil ->
        GenServer.cast(self(), message)
        {:reply, :ok, state}
      _log ->
        {:reply, :config_change_in_progress, state}
    end
  end
  def handle_call(message = {:config_change, _}, _from, state = %{leader: leader}) when not is_nil(leader) do
    # IO.inspect(state, label: "LogEntry :config_change")
    {:reply, GenServer.call(state.leader, message, 512), state}
  end
  def handle_call({:config_change, _}, _from, state) do
    {:reply, :no_leader, state}
  end

  # Retrieves the last log with configuration change
  def get_config_nodes(nodes, logs) do
    log = logs |> Enum.find(fn
      (%{type: :config_change, command: {_a, _b}}) -> true
      _ -> false
    end)

    if is_nil(log) do
      {nodes, [], nil}
    else
      {log.command |> elem(0), log.command |> elem(1), log}
    end
  end

  def get_config_nodes_v2(nodes, committed_index, logs) do
    uncommitted_log = logs
    |> Enum.take_while(fn (%{index: index}) -> index > committed_index end)
    |> Enum.find(fn
      (%{type: :config_change, command: {_a, _b}}) -> true
      _ -> false
    end)

    case uncommitted_log do
      nil ->
        committed_log = logs
        |> Enum.drop_while(fn (%{index: index}) -> index > committed_index end)
        |> Enum.find(fn
          (%{type: :config_change, command: {_a, _b}}) -> true
          _ -> false
        end)
        case committed_log do
          nil -> {nodes, [], nil}
          log -> {log.command |> elem(1), [], nil}
        end

      log ->
        {log.command |> elem(0), log.command |> elem(1), log}
    end

  end

  def nodes_removed(nodes) do
    nodes
    |> Enum.each(fn (node) ->
      GenServer.cast(node, {:removed, self()})
    end)
  end
  # added nodes will be followers, atleast initially.
  def nodes_added(_nodes) do
  end

  def append_log(log, state = %{
    state: :leader,
    term: term,
    logs: logs,
    next_indexes: next_indexes,
    match_indexes: match_indexes,
    nodes: nodes,
    committed_index: committed_index
  }) do
    new_logs = [log | logs]

    nodes
    |> Enum.reject(fn (node) -> Exra.Utils.is_self?(node) end)
    |> Enum.map(fn (node) ->
      next_index = Map.get(next_indexes, node, 1)
      logs_to_send = Enum.take_while(new_logs, fn (%{index: index}) -> index >= next_index end)
      previous_log = Enum.find(logs, fn %{index: index} -> index == (next_index - 1) end)


      GenServer.cast(node, {:replicate, logs_to_send, %{
        index: previous_log.index,
        term: previous_log.term
      }, self(), term, committed_index})
    end)

    # Ensure match indexes exist for all nodes except myself
    match_indexes = nodes
    |> Enum.reject(fn (node) -> Exra.Utils.is_self?(node) end)
    |> Enum.map(fn node ->
      Task.async(fn -> Exra.Utils.resolve_pid(node) end)
    end)
    |> Task.await_many()
    |> Enum.reduce(match_indexes, fn (pid, acc) ->
      case is_nil(pid) || acc |> Map.has_key?(pid) do
        true -> acc
        false -> acc |> Map.put(pid, 0)
      end
    end)

    {:noreply, %{state |
      logs: new_logs,
      match_indexes: match_indexes
    }}
  end

  defp compact_logs(logs, committed_index) do
    {uncommitted, committed} = Enum.split_while(logs, fn log -> log.index > committed_index end)

    {compacted_committed, _seen_keys, _seen_config} =
      Enum.reduce(committed, {[], MapSet.new(), false}, fn log, {acc, seen_keys, seen_config} ->
        case log.type do
          :command ->
            case log.command do
              {key, _value} ->
                if MapSet.member?(seen_keys, key) do
                  {acc, seen_keys, seen_config}
                else
                  {[log | acc], MapSet.put(seen_keys, key), seen_config}
                end
              _ ->
                {[log | acc], seen_keys, seen_config}
            end

          :config_change ->
            if seen_config do
              {acc, seen_keys, seen_config}
            else
              {[log | acc], seen_keys, true}
            end

          _ ->
            {[log | acc], seen_keys, seen_config}
        end
      end)

    uncommitted ++ Enum.reverse(compacted_committed)
  end

end
