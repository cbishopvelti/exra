defmodule Exra.ClusterObserver do
  use GenServer

  require Logger

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    :net_kernel.monitor_nodes(true)

    Process.whereis(Exra) |> IO.inspect(label: "303 CLusterObserver.init")

    {:ok, nil}
  end

  @impl true
  def handle_info({
    :nodeup,
    node
    # node = :"a@127.0.0.1"
  }, state) do
    IO.puts("Node joined: #{inspect(node)}")

    my_pid = Process.whereis(Exra)
    their_pid = :rpc.call(node, Process, :whereis, [Exra])
    exra_state = my_pid |> GenServer.call(:get_state)
    case GenServer.call(my_pid, {:config_change, {[my_pid | exra_state.nodes], [ their_pid, my_pid | exra_state.nodes] }}) do
      :ok -> nil
      :no_leader ->
        Logger.debug("No leader, retrying in 256ms")
        Process.send_after(self(), {:nodeup, node}, 256)
    end

    my_pid |> GenServer.call(:get_state) |> IO.inspect(label: "001 my")
    their_pid |> GenServer.call(:get_state) |> IO.inspect(label: "002 their")

    {:noreply, state}
  end
  def handle_info({:nodeup, _node}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, _node}, state) do
    {:noreply, state}
  end
end
