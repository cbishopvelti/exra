defmodule LibclusterExample.MyStateMachine do
  @behaviour Exra.StateMachine

  def leader(_term, _node_length) do
    IO.puts("I've become a LEADER")
  end

  def follower(_term, _node_length) do
    IO.puts("I've become a FOLLOWER")
  end

  def removed(_by) do

  end
end
