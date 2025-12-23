defmodule Exra do
  use GenServer, restart: :transient

  require Logger

  alias Exra.LogEntry

  def start_link(args) do
    name = Keyword.get(args, :name, __MODULE__)

    init_args = args
    |> Enum.into(%{
      init_nodes: []
    })
    |> Map.merge(%{
      name: name
    })

    GenServer.start_link(__MODULE__, init_args, name: name)
  end


  defmacro get_quorum(nodes) do
    quote do
      # max(div(length(unquote(nodes)), 2) + 1, 2)
      div(length(unquote(nodes)), 2) + 1
    end
  end

  @type state :: %{
    name: String.t(),
    state: :candidate | :follower | :leader,
    term: integer(),
    voted_for: pid() | nil,
    timeout_min: integer(),
    timeout_max: integer(),
    ping_interval: integer(),
    timeout: reference(),
    nodes: [pid()],
    auto_tick: boolean(),
    state_machine: Exra.StateMachine.t() | nil,
    logs: [LogEntry.t()],
    next_indexes: %{},
    match_indexes: %{},
    committed_index: integer(),
    applied_index: integer(),
    subscriber: pid() | nil,
    leader: pid() | nil
  }

  def init(args) do
    state = %{
      name: args.name,
      pid: self(),
      state: Map.get(args, :state, :follower),
      votes: 0,
      term: 0,
      voted_for: nil,
      timeout_min: 350,
      timeout_max: 450,
      ping_interval: 250,
      nodes: args.init_nodes,
      timeout: nil,
      auto_tick: Map.get(args, :auto_tick, true),
      state_machine: Map.get(args, :state_machine,
        Application.get_env(:exra, :state_machine)
      ) |> then(&(&1 && Code.ensure_loaded!(&1))),
      logs: [%{
        index: 0,
        term: 0,
        command: nil,
        type: :genesis
      }],
      next_indexes: %{},
      match_indexes: %{},
      committed_index: 0,
      applied_index: 0,
      subscriber: Map.get(args, :subscriber),
      leader: nil
    }

    # if state.state == :follower do
    #   Exra.Utils.notify_state_machine(state, :follower, [state.term, length(state.nodes) + 1])
    # end

    {:ok, %{state | timeout: tick(:timeout, state) }}
  end


  # No nodes, so become leader
  def handle_info(:timeout, state = %{nodes: [], term: term}) do
    # IO.puts(":timeout -----")
    !is_nil(state.timeout) && Process.cancel_timer(state.timeout)
    timeout = tick(:send_heartbeat, state)
    new_term = term + 1
    Exra.Utils.notify_state_machine(state, :leader, [new_term, 1])
    {:noreply, %{state | state: :leader, term: new_term, voted_for: nil,
      votes: 1, timeout: timeout, leader: nil
    }}
  end
  def handle_info(:timeout, state = %{logs: [log | _]}) do

    !is_nil(state.timeout) && Process.cancel_timer(state.timeout)

    new_term = state.term + 1
    __MODULE__.broadcast_from({
      :candidate,
      self(),
      new_term,
      log.term,
      log.index
    }, state)

    Logger.debug(":timeout happened, becoming candidate #{new_term}")

    timeout = tick(:timeout, state)
    # Process.send_after(self(), :timeout, Enum.random(state.timeout_min..state.timeout_max))

    {:noreply, %{state | state: :candidate, term: new_term, votes: 1,
      voted_for: nil, timeout: timeout, leader: nil
    }}
  end

  def handle_info(:send_heartbeat, state) do
    state.timeout && Process.cancel_timer(state.timeout)
    send_heartbeat(state.term, state)

    # timeout = Process.send_after(self(), :send_heartbeat, state.ping_interval)
    timeout = tick(:send_heartbeat, state)

    {:noreply, %{state | timeout: timeout}}
  end

  # Called after startup with the nodes for this cluster
  def handle_cast({:init_set_nodes, nodes}, state) do
    timeout = tick(:timeout, state)
    {:noreply, %{state | nodes: nodes, timeout: timeout}}
  end

  def handle_cast(message = {:candidate, _from, _their_term, _log_term, _log_index}, state),
    do: Exra.Candidate.handle_cast(message, state)

  def handle_cast(message = {:append_from_user, _command}, state),
    do: LogEntry.handle_cast(message, state)
  def handle_cast(message = {:append_from_user, _command, _from}, state),
      do: LogEntry.handle_cast(message, state)
  def handle_cast(message = {:config_change, _command}, state),
    do: LogEntry.handle_cast(message, state)
  def handle_cast(message = {:replicated, _index, _from}, state),
    do: LogEntry.handle_cast(message, state)
  def handle_cast(message = {:replicate, _log, _previous_log, _from, _term, _committed_index}, state),
    do: LogEntry.handle_cast(message, state)
  def handle_cast(message = {:committed_index, _index}, state),
    do: LogEntry.handle_cast(message, state)

  def handle_cast({:vote, true, term}, state = %{term: term, state: :candidate, votes: votes,
    nodes: nodes
  }) when votes + 1 >= get_quorum(nodes) do
    Logger.debug("I've become the leader", [term: term, name: state.name])

    !is_nil(state.timeout) && Process.cancel_timer(state.timeout)
    send_heartbeat(term, state)
    timeout = tick(:send_heartbeat, state)

    # broadcast_from({:follower, term, self()}, state)
    Exra.Utils.notify_state_machine(state, :leader, [term, length(nodes) + 1])

    # We've won the vote, become the leader.
    {:noreply, %{state | votes: votes + 1, state: :leader, timeout: timeout}}
  end
  def handle_cast({:vote, true, term}, state = %{term: term, state: :candidate, votes: votes}) do
    Logger.debug(":vote received a vote", [term: term, name: state.name])
    {:noreply, %{state| votes: votes + 1}}
  end
  def handle_cast({:vote, false, term}, state = %{term: my_term}) when term > my_term do
    Logger.debug(":vote, received rejected vote")
    {:noreply, %{state |
      state: :follower,
      term: term,
      voted_for: nil
    }}
  end
  def handle_cast({:vote, _a, term}, state = %{term: my_term}) do
    Logger.debug(":vote has finished or received rejected vote, their_term: #{term}, my_term: #{my_term}")
    # Voting has finished.
    {:noreply, state}
  end

  # Manual tick for testing, only call if auto_tick is false
  def handle_cast({:tick, message}, state = %{auto_tick: false}) do
    Process.cancel_timer(state.timeout)
    send(self(), message)
    {:noreply, %{state | timeout: nil }}
  end
  # Tell state_machine they're a follower
  def handle_cast({:follower, term, from}, state = %{nodes: nodes}) do
    Exra.Utils.notify_state_machine(state, :follower, [term, length(nodes) + 1])
    {:noreply, %{state | leader: from}}
  end
  def handle_cast({:stale, term, from}, state) do
    !is_nil(state.timeout) && Process.cancel_timer(state.timeout)
    timeout = Exra.tick(:timeout, state)

    {:noreply, %{ state |
      state: :follower,
      voted_for: nil,
      term: term,
      timeout: timeout,
      leader: from
    }}
  end
  def handle_cast({:removed, by}, state = %{timeout: timeout}) do
    !is_nil(timeout) && Process.cancel_timer(timeout)
    # !is_nil(subscriber) && send(subscriber, {:removed, self(), by})
    Exra.Utils.notify_state_machine(state, :removed, [by])

    Logger.warning("Process stopped #{inspect{state.nodes}}")
    {:stop, :normal, state}
    # {:noreply, %{state |
    #   timeout: nil
    # }}
  end

  # get_state for testing
  def handle_call(:get_state, {_from, _}, state) do
    {:reply, state, state}
  end
  # Only call on leader
  def handle_call({:new_nodes, new_nodes}, {_from, _}, state = %{logs: logs, nodes: nodes, term: term,
    committed_index: committed_index, state: :leader
  }) do
    # new_state = %{state |
    #   nodes: MapSet.union(new_nodes |> MapSet.new(), state.nodes |> MapSet.new()) |> MapSet.to_list(),
    #   logs: [%{
    #     term: term,
    #     index: hd(logs).index + 1,
    #     type: :config_change,
    #     command: {nodes, new_nodes}
    #   } | logs]
    # }

    fresh_nodes = MapSet.difference(
      new_nodes |> MapSet.new(),
      [ self() | state.nodes] |> MapSet.new()
    ) |> MapSet.to_list()
    # Catchup fresh nodes
    fresh_nodes
    |> Enum.map(fn (node) ->
      GenServer.cast(node, {:replicate, logs |> List.delete_at(-1), %{
        index: (logs |> List.last()).index,
        term: (logs |> List.last()).term
      }, self(), term, committed_index })
    end)

    # Commit the config change
    GenServer.cast(self(), {:config_change, {[self() | nodes], new_nodes}})

    {:reply, :ok, state}
  end
  def handle_call(message = {:config_change, _}, from, state), do: Exra.LogEntry.handle_call(message, from, state)
  def handle_call(message = {:command, _}, from, state), do: Exra.LogEntry.handle_call(message, from, state)

  def tick(:timeout, state = %{auto_tick: true}) do
    # IO.puts("tick :timeout")
    Process.send_after(self(), :timeout, Enum.random(state.timeout_min..state.timeout_max))
  end
  def tick(:send_heartbeat, state = %{auto_tick: true}) do
    # IO.puts("tick :send_heartbeat")
    Process.send_after(self(), :send_heartbeat, state.ping_interval)
  end
  def tick(_message, _state = %{auto_tick: false}) do
    nil
  end

  def broadcast_from(message, state) do
    # IO.inspect(state.nodes, label: "002 broadcast_from")
    state.nodes
    |> Enum.reject(fn (pid) -> Exra.Utils.is_self?(pid) end )
    |> Enum.each(fn (node) ->
      GenServer.cast(node, message)
    end)
  end

  def send_heartbeat(term, state = %{logs: [log | _], committed_index: committed_index}) do
    __MODULE__.broadcast_from({:replicate, [], %{
      index: log.index,
      term: log.term
    }, self(), term, committed_index}, state)
    :ok
  end

  @doc"""
  Append a log. Ideally would be a {key, value} tuple
  """
  def command(command) do
    {Exra, Node.self()}
    |> GenServer.call({:command, command})
  end
end
