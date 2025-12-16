defmodule Exra do
  use GenServer

  require Logger

  alias Exra.LogEntry

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: Map.get(args, :name, __MODULE__))
  end


  defmacro get_quarum(nodes) do
    quote do
      max(div(length(unquote(nodes)), 2) + 1, 2)
    end
  end

  @type state :: %{
    name: String.t(),
    state: :candidate | :follower | :leader,
    term: integer(),
    timeout_min: integer(),
    timeout_max: integer(),
    ping_interval: integer(),
    timeout: reference(),
    nodes: [pid()],
    state_machine: any(),
    auto_tick: boolean(),
    state_machine: Exra.StateMachine.t() | nil,
    logs: [LogEntry.t()],
    next_indexes: %{},
    match_indexes: %{},
    commit_index: integer(),
    applied_index: integer(),
    subscriber: pid() | nil
  }

  def init(args) do

    state = %{
      name: args.name,
      pid: self(),
      state: :follower,
      votes: 0,
      term: 0,
      timeout_min: 350,
      timeout_max: 450,
      ping_interval: 250,
      nodes: args.init_nodes,
      timeout: nil,
      auto_tick: Map.get(args, :auto_tick, true),
      state_machine: Map.get(args, :state_machine, nil) |> then(&(&1 && Code.ensure_loaded!(&1))),
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
      subscriber: Map.get(args, :subscriber)
    }

    {:ok, state}
  end

  def handle_info(:timeout, _state = %{nodes: []}) do
    raise "Nodes can't be empty, as it should atleast have oneself"
  end
  def handle_info(:timeout, state = %{nodes: [_self], term: term}) do
    !is_nil(state.timeout) && Process.cancel_timer(state.timeout)
    # Only node, so become leader straight away
    timeout = tick(:send_ping, state)
    new_term = term + 1
    if function_exported?(state.state_machine, :leader, 1) do
      apply(state.state_machine, :leader, [new_term])
    end
    {:noreply, %{state | state: :leader, term: new_term, votes: 1, timeout: timeout}}
  end
  def handle_info(:timeout, state) do

    !is_nil(state.timeout) && Process.cancel_timer(state.timeout)

    new_term = state.term + 1
    __MODULE__.broadcast_from({
      :candidate,
      self(),
      new_term
    }, state)

    timeout = tick(:timeout, state)
    # Process.send_after(self(), :timeout, Enum.random(state.timeout_min..state.timeout_max))

    {:noreply, %{state | state: :candidate, term: new_term, votes: 1, timeout: timeout }}
  end
  def handle_info(:send_ping, state) do
    state.timeout && Process.cancel_timer(state.timeout)
    ping(state.term, state)

    # timeout = Process.send_after(self(), :send_ping, state.ping_interval)
    timeout = tick(:send_ping, state)

    {:noreply, %{state | timeout: timeout}}
  end

  # Called after startup with the nodes for this cluster
  def handle_cast({:init_set_nodes, nodes}, state) do
    timeout = tick(:timeout, state)
    {:noreply, %{state | nodes: nodes, timeout: timeout}}
  end
  # Their term is less we've either already voted, or something else has taken precedence.
  def handle_cast({:candidate, _from, their_term}, state) when their_term <= state.term do
    {:noreply, state}
  end
  def handle_cast({:candidate, from, their_term}, state) when their_term > state.term do
    GenServer.cast(from, {
      :vote,
      their_term
    })

    !is_nil(state.timeout) && Process.cancel_timer(state.timeout)
    timeout = tick(:timeout, state)

    # IO.inspect(their_term, label: "001 #{state.name}")
    {
      :noreply, %{state | term: their_term,
      timeout: timeout}}
  end
  def handle_cast({:candidate, _from, _their_term}, state) do
    # Invalid, do nothing
    {:noreply, state}
  end
  def handle_cast(message = {:append_from_user, _command}, state),
    do: LogEntry.handle_cast(message, state)
  def handle_cast(message = {:replicated, _index, _from}, state),
    do: LogEntry.handle_cast(message, state)
  def handle_cast(message = {:replicate, _log, _previous_log, _from}, state),
    do: LogEntry.handle_cast(message, state)
  def handle_cast(message = {:committed_index, _index}, state),
    do: LogEntry.handle_cast(message, state)

  def handle_cast({:vote, term}, state = %{term: term, state: :candidate, votes: votes,
  nodes: nodes, subscriber: subscriber}) when votes + 1 >= get_quarum(nodes) do
    Logger.debug("I've become the leader", [term: term, name: state.name])

    !is_nil(state.timeout) && Process.cancel_timer(state.timeout)
    ping(term, state)
    timeout = tick(:send_ping, state)

    broadcast_from({:follower, term}, state)
    if function_exported?(state.state_machine, :leader, 1) do
      apply(state.state_machine, :leader, [term])
    end
    !is_nil(subscriber) && send(subscriber, {:leader, self()})


    # We've won the vote, become the leader.
    {:noreply, %{state | votes: votes + 1, state: :leader, timeout: timeout}}
  end
  def handle_cast({:vote, term}, state = %{term: term, state: :candidate, votes: votes}) do
    Logger.debug("I've received a vote", [term: term, name: state.name])
    {:noreply, %{state| votes: votes + 1}}
  end
  def handle_cast({:vote, _}, state) do
    # Voting has finished.
    {:noreply, state}
  end

  def handle_cast({:ping, term, _from}, state) when term >= state.term do
    !is_nil(state.timeout) && Process.cancel_timer(state.timeout)
    timeout = tick(:timeout, state)

    {:noreply, %{state | state: :follower, timeout: timeout, term: term}}
  end
  # Manual tick for testing, only call if auto_tick is false
  def handle_cast({:tick, message}, state = %{auto_tick: false}) do
    Process.cancel_timer(state.timeout)
    send(self(), message)
    {:noreply, %{state | timeout: nil }}
  end
  # Tell everyone they're a follower
  def handle_cast({:follower, term}, state = %{subscriber: subscriber}) do
    if function_exported?(state.state_machine, :leader, 1) do
      apply(state.state_machine, :follower, [term])
    end
    !is_nil(subscriber) && send(subscriber, {:follower, self()})
    {:noreply, state}
  end

  defp tick(:timeout, state = %{auto_tick: true}) do
    Process.send_after(self(), :timeout, Enum.random(state.timeout_min..state.timeout_max))
  end
  defp tick(:send_ping, state = %{auto_tick: true}) do
    Process.send_after(self(), :send_ping, state.ping_interval)
  end
  defp tick(_message, _state = %{auto_tick: false}) do
    nil
  end

  # get_state for testing
  def handle_call(:get_state, {_from, _}, state) do
    # IO.inspect(state.term, label: "002 #{state.name}")
    {:reply, state, state}
  end

  def broadcast_from(message, state) do
    state.nodes
    |> Enum.filter(fn (node) -> node != self() end )
    |> Enum.each(fn (node) ->
      GenServer.cast(node, message)
    end)
  end

  def ping(term, state) do
    __MODULE__.broadcast_from({:ping, term, self()}, state)
    :ok
  end


end
