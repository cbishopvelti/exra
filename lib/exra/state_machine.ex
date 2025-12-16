defmodule StateMachine do
  @type t :: module()

  @callback leader() ::any()
  @callback follower() ::any()
end
