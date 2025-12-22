defmodule LibclusterExample.MyStateMachine do
  @behaviour Exra.StateMachine
  require Logger

  def leader(_term, _node_length) do
    IO.puts("I've become a LEADER")
  end

  def follower(_term, _node_length) do
    # IO.puts("I've become a FOLLOWER")
    Logger.debug("MyStateMachine, I've become a Follower")
    # Process.info(self(), :current_stacktrace) |> IO.inspect(label: "MyStateMachine, follower")
  end

  def removed(_by) do

  end
end
