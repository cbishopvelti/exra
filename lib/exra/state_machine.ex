defmodule Exra.StateMachine do
  @moduledoc """
  Callbacks for the StateMachine, the Statemachine is run in process, so don't block here.
  """

  @type t :: module()

  @doc """
  Callback invoked when this node is becomes the **Leader**.

  ## Parameters
  * `term` - The current election term (a monotonically increasing integer).
  * `nodes` - An integer representing the current number of nodes
  """
  @callback leader(term:: Integer, nodes:: Integer) ::any()
  @doc """
  Callback invoked when this node becomes a **Follower**.
  If the node is initialized as a follower, it wont run until another node is elected leader. This avoids receiving a follower event directly followed by a leader event.
  """
  @callback follower(term ::Integer, nodes ::Integer) ::any()

  @doc """
  Callback invoked when a logs are committed.
  """
  @callback committed(%{
    pid: pid(),
    new_committed_index: integer(),
    old_committed_index: integer(),
    logs: [Exra.LogEntry.t()],
    role: String.t()
  }) ::any()

  @doc """
  This is run when a node is removed from the cluster, this could happen on configuration change
  """
  @callback removed(by ::pid()) ::any()
end
