defmodule Exra.ClusterObserver do
  use GenServer

  require Logger

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    :net_kernel.monitor_nodes(true)

    # 2. Get the snapshot of who is already here
    initial_peers = Node.list()

    # 3. Store them in a MapSet for fast lookups
    state = %{
      suppressed_nodes: MapSet.new(initial_peers)
    }

    {:ok, state}
  end

  @impl true
  def handle_info({
    :nodeup,
    node
    # node = :"a@127.0.0.1"
  }, state) do
    if MapSet.member?(state.suppressed_nodes, node) do
      # CASE A: This node was in Node.list() during init.
      # We ignore it to satisfy "never fire on rejoin".
      {:noreply, state}
    else
      IO.puts("Node joined: #{inspect(node)} ===================")

      # my_pid = Process.whereis(Exra)
      # their_pid = :rpc.call(node, Process, :whereis, [Exra])
      my_addr = {Exra, Node.self()}
      their_addr = {Exra, node}

      exra_state = my_addr |> GenServer.call(:get_state) # |> IO.inspect(label: "001")
      case GenServer.call(my_addr, {:config_change, {
        [my_addr | exra_state.nodes],
        [ their_addr, my_addr | exra_state.nodes] |> Enum.uniq()
      }}) do
        :ok ->
          Logger.debug(":Config change :ok")
          nil
        :config_change_in_progress ->
          Logger.debug("Config change in progress, retrying in 1m # 128ms")
          Process.send_after(self(), {:nodeup, node}, 60_000)
        :no_leader ->
          Logger.debug("No leader, retrying in 256ms")
          Process.send_after(self(), {:nodeup, node}, 256)
      end
      {:noreply, state}
    end
  end
  def handle_info({
    :nodeup,
    _node
  }, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    new_suppressed = MapSet.delete(state.suppressed_nodes, node)
    IO.puts("Node lost: #{inspect(node)}")
    {:noreply, %{state | suppressed_nodes: new_suppressed}}
  end
end
