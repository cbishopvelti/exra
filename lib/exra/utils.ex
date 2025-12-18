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
end
