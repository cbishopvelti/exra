defmodule Exra.LogEntry do
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

  # Only call this on the leader
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
  def handle_cast({:config_change, command}, state = %{
    state: :leader,
    term: term,
    logs: logs
  }) do
    log = %{
      term: term,
      index: hd(logs).index + 1,
      command: command,
      type: :config_change
    }
    append_log(log, state)
  end
  def handle_cast({:replicated, false, from}, state = %{state: :leader, next_indexes: next_indexes, logs: logs, term: term}) do
    # step back and send previous logs
    ni = (next_indexes |> Map.get(from)) - 1
    new_next_indexes = next_indexes |> Map.put(from, ni)

    node = from
    logs_to_send = Enum.take_while(logs, fn (%{index: index}) -> index >= ni end)
    previous_log = Enum.find(logs, fn %{index: index} -> index == (max(1, ni) - 1) end)

    GenServer.cast(node, {:replicate, logs_to_send, %{
      index: previous_log.index,
      term: previous_log.term
    }, self(), term})

    {:noreply, %{state | next_indexes: new_next_indexes }}
  end
  def handle_cast({:replicated, index, from}, state = %{
    state: :leader, next_indexes: next_indexes, logs: logs = [log | _],
    match_indexes: match_indexes, committed_index: committed_index, nodes: nodes,
    subscriber: subscriber
  }) do
    new_next_indexes = next_indexes |> Map.put(from, index + 1)
    new_match_indexes = match_indexes |> Map.put(from, index)

    { c_old, c_new, c_log} = get_config_nodes(
      nodes,
      logs |> Enum.take_while(fn (%{index: index}) -> index > committed_index end)
    )
    c_old_new_match_indexes = Map.take(new_match_indexes, c_old)
    c_new_new_match_indexes = Map.take(new_match_indexes, c_new)

    c_old_commit = [log.index | (c_old_new_match_indexes |> Map.values())] |> median()
    c_new_commit = [log.index | (c_new_new_match_indexes |> Map.values())] |> median()

    new_committed_index = min(c_old_commit, c_new_commit)

    nodes = if (!is_nil(c_log) and c_log.index <= new_committed_index) do # Config change is fully applied, so old nodes are no longer part of the cluster
      nodes_removed(MapSet.difference(
        nodes |> MapSet.new(),
        c_new |> MapSet.new()
      ) |> MapSet.to_list())
      c_new
    else
      nodes
    end

    case new_committed_index > committed_index do
      true ->
        nodes
        |> Enum.filter(fn (node) -> node != self() end)
        |> Enum.each(fn (node) ->
          GenServer.cast(node, {:committed_index, new_committed_index})
        end)
        # TODO, send to state machine
        !is_nil(subscriber) && send(subscriber, {:committed, %{
          pid: self(),
          new_committed_index: new_committed_index,
          old_committed_index: committed_index,
          state: state.state
        }})
      false -> # Nothings changed, don't tell anyone
        nil
    end


    {:noreply, %{state | next_indexes: new_next_indexes, match_indexes: new_match_indexes, committed_index: new_committed_index,
      nodes:  nodes
    }}
  end

  # Replicate/heartbeat
  def handle_cast({:replicate, _logs, _previous_log, from, term, _committed_index}, state = %{state: :follower, logs: _my_logs, term: my_term}) when term < my_term do
    GenServer.cast(from, {:stale, my_term})
    {:noreply, state}
  end
  def handle_cast({:replicate, logs, previous_log, from, term, committed_index}, state = %{state: my_state, logs: my_logs, term: my_term,
    timeout: timeout, subscriber: subscriber, committed_index: my_committed_index})
  do

    !is_nil(timeout) && Process.cancel_timer(timeout)
    timeout = Exra.tick(:timeout, state)

    state = if term >= state.term and state.state != :follower do
        new_state = %{state | state: :follower, term: term, voted_for: (if term > state.term, do: nil, else: state.voted_for)}
        notify_state_machine(state, :follower, term)
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
          # TODO tell state machine
          !is_nil(subscriber) && send(subscriber, {:committed, %{
            pid: self(),
            new_committed_index: committed_index,
            old_committed_index: my_committed_index,
            state: my_state
          }})
        end

        out
      false ->
        # Tell the leader we don't match and to step back the next_index
        GenServer.cast(from, {:replicated, false, self()})
        my_logs # my old logs, nothings changed
    end

    {:noreply, %{state |
      logs: new_logs,
      state: :follower,
      term: term,
      voted_for: (if term > my_term, do: nil, else: state.voted_for),
      timeout: timeout,
      committed_index: committed_index
    }}
  end


  def handle_cast({:committed_index, committed_index}, state = %{logs: [log | _], subscriber: subscriber, committed_index: old_committed_index}) do
    new_committed_index = min(committed_index, log.index)
    if (new_committed_index != old_committed_index) do
      # TODO: Send to state machine
      !is_nil(subscriber) && send(subscriber, {:committed, %{
        pid: self(),
        new_committed_index: new_committed_index,
        old_committed_index: old_committed_index,
        state: state.state
      }})
    end

    {:noreply, %{state | committed_index: new_committed_index}}
  end

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

  def median(indexes) do
    sorted_matches = indexes |> Enum.sort(:desc)
    middle_index = sorted_matches |> length() |> div(2)
    median = Enum.at(sorted_matches, middle_index)
    median
  end

  def nodes_removed(nodes) do
    nodes
    |> Enum.each(fn (node) ->
      GenServer.cast(node, {:removed, self()})
    end)
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
    |> Enum.filter(fn (node) -> node != self() end)
    |> Enum.map(fn (node) ->
      ni = Map.get(next_indexes, node, 1)
      logs_to_send = Enum.take_while(new_logs, fn (%{index: index}) -> index >= ni end)
      previous_log = Enum.find(logs, fn %{index: index} -> index == (ni - 1) end)

      GenServer.cast(node, {:replicate, logs_to_send, %{
        index: previous_log.index,
        term: previous_log.term
      }, self(), term, committed_index})
    end)

    # Ensure match indexes exist for all nodes except myself
    match_indexes = nodes
    |> Enum.filter(fn (node) -> node != self() end)
    |> Enum.reduce(match_indexes, fn (node, acc) ->
      case acc |> Map.has_key?(node) do
        true -> acc
        false -> acc |> Map.put(node, 0)
      end
    end)

    {:noreply, %{state |
      logs: new_logs,
      match_indexes: match_indexes
    }}
  end
  def notify_state_machine(state = %{subscriber: subscriber}, function, term) do
    if function_exported?(state.state_machine, function, 1) do
      apply(state.state_machine, function, [term])
    end
    !is_nil(subscriber) && send(subscriber, {function, self()})
  end
end
