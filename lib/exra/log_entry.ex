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

  def handle_cast({:append_from_user, command}, state = %{
    state: :leader,
    term: term,
    logs: logs,
    next_indexes: next_indexes,
    match_indexes: match_indexes,
    nodes: nodes
  }) do
    index = length(state.logs)

    log = %{
      term: term,
      index: index,
      command: command,
      type: :command
    }
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
      }, self()})
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
  def handle_cast({:replicated, false, from}, state = %{state: :leader, next_indexes: next_indexes, logs: logs}) do
    # step back and send previous logs
    ni = (next_indexes |> Map.get(from)) - 1
    new_next_indexes = next_indexes |> Map.put(from, ni)

    node = from
    logs_to_send = Enum.take_while(logs, fn (%{index: index}) -> index >= ni end)
    previous_log = Enum.find(logs, fn %{index: index} -> index == (max(1, ni) - 1) end)

    GenServer.cast(node, {:replicate, logs_to_send, %{
      index: previous_log.index,
      term: previous_log.term
    }, self()})

    {:noreply, %{state | next_indexes: new_next_indexes }}
  end
  def handle_cast({:replicated, index, from}, state = %{
    state: :leader, next_indexes: next_indexes, logs: [log | _],
    match_indexes: match_indexes, committed_index: committed_index, nodes: nodes,
    subscriber: subscriber
  }) do
    new_next_indexes = next_indexes |> Map.put(from, index + 1)
    new_match_indexes = match_indexes |> Map.put(from, index)

    sorted_matches = [log.index | (new_match_indexes |> Map.values())] |> Enum.sort(:desc)
    middle_index = sorted_matches |> length() |> div(2)
    new_committed_index = Enum.at(sorted_matches, middle_index)

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


    {:noreply, %{state | next_indexes: new_next_indexes, match_indexes: new_match_indexes, committed_index: new_committed_index}}
  end

  def handle_cast({:replicate, logs, previous_log, from}, state = %{state: :follower, logs: my_logs}) do

    # previous_index = logs
    # |> Enum.find_index(fn (log) -> previous_log.index == log.index end)
    previous_index = previous_log.index

    my_previous_log = my_logs |> Enum.find(fn (log) -> log.index == previous_index end)

    new_logs = case !is_nil(my_previous_log) && previous_log.term == my_previous_log.term do
      true -> # all is fine, commit
        out = logs ++ Enum.drop_while(my_logs, fn (log) -> log.index > previous_index end)

        GenServer.cast(from, {:replicated, hd(out).index, self()})
        out
      false ->
        # Tell the leader we don't match and to step back the next_index
        GenServer.cast(from, {:replicated, false, self()})
        my_logs # my old logs, nothings changed
    end


    {:noreply, %{state | logs: new_logs}}
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

end
