defmodule Exra.StateMachine do
  @type t :: module()

  @callback leader(term:: Integer, nodes:: Integer) ::any()
  @callback follower(term ::Integer, nodes ::Integer) ::any()
  @callback removed(by ::pid()) ::any()
end
