defmodule Exra.Utils do
  def median(indexes) do
    sorted_matches = indexes |> Enum.sort(:desc)
    middle_index = sorted_matches |> length() |> div(2)
    median = Enum.at(sorted_matches, middle_index)
    median
  end

  def notify_state_machine(state = %{subscriber: subscriber}, function, args) do
    if function_exported?(state.state_machine, function, length(args)) do
      apply(state.state_machine, function, args)
    end
    !is_nil(subscriber) && send(subscriber, {function, self(), args})
  end

  def is_self?(pid) when is_pid(pid), do: pid == self()
  def is_self?({name, node}), do: node == node() and Process.whereis(name) == self()
  def is_self?(_), do: false

  def resolve_pid(pid) when is_pid(pid), do: pid
  def resolve_pid({name, node}) when node == node() do
    Process.whereis(name)
  end
  def resolve_pid({name, node}) do
    # Perform a Remote Procedure Call to look up the name on the remote node
    case :rpc.call(node, Process, :whereis, [name], 50) do
      x when is_pid(x) -> x
      _ -> nil # Failed getting the pid, so just return nil
    end
  end
end
