defmodule Exra.Candidate do
require Logger

  def handle_cast( {:candidate, from, their_term, their_log_term, their_log_index}, state) do
    state = %{term: my_term, nodes: nodes, state: role} = maybe_step_down(their_term, state)

    with {:in_our_cluster, true} <- {:in_our_cluster, from in nodes},
      {:can_vote, true} <- {:can_vote, role in [:follower]},
      {:valid_term, true} <- {:valid_term, valid_term(their_term,  my_term)},
      {:not_voted, true} <- {:not_voted, not_voted(their_term, from, state)},
      {:valid_logs, true} <- {:valid_logs, valid_logs(their_log_term, their_log_index, state)}

    do
      new_state = vote_for_candidate({from, true, their_term}, state)
      {:noreply, new_state}
    else
      {_, false } ->
        new_state = vote_for_candidate({from, false, max(their_term, my_term)}, state)
        {:noreply, new_state}
    end
  end

  defp maybe_step_down(their_term, state = %{term: my_term, state: role, timeout: timeout}) when their_term > my_term do
    Logger.debug("Candidate.maybe_step_down")
    new_role = (if role == :learner, do: :learner, else: :follower)

    # We have just become or where initiated as a follower, so notify.
    if (my_term == 0 or state.state != new_role) do
      Exra.Utils.notify_state_machine(state, :follower, [their_term, length(state.nodes) + 1])
    end

    timeout = if (role !== :learner) do
      !is_nil(state.timeout) && Process.cancel_timer(state.timeout)
      Exra.tick(:timeout, state)
    else
      timeout
    end

    %{state | voted_for: nil,
      term: their_term,
      state: new_role,
      timeout: timeout
    }
  end
  defp maybe_step_down(_a, state) do
    state
  end


  defp valid_term(their_term, my_term) when their_term >= my_term do
    true
  end
  defp valid_term(_their_term, _my_term) do
    false
  end

  defp not_voted(their_term, _from, state) when their_term > state.term do
    true
  end
  defp not_voted(term, _from, %{voted_for: nil, term: term}) do
    true
  end
  defp not_voted(term, from, %{voted_for: from, term: term}) do
    true
  end
  defp not_voted(_, _, _) do
    false
  end

  defp valid_logs(their_log_term, their_log_index, %{logs: [%{term: my_term, index: my_index} | _]})
    when their_log_term > my_term or (their_log_term == my_term and their_log_index >= my_index)
  do
    true
  end
  defp valid_logs(_, _, %{logs: []}), do: raise "Shouldnt happen, empty logs, as there should always be a genesis log"
  defp valid_logs(_, _, _), do: false


  def vote_for_candidate({from, success, term}, state) do

    GenServer.cast(from, {
      :vote,
      success,
      term
    })

    if success do
      if state.timeout, do: Process.cancel_timer(state.timeout)
      new_timeout = Exra.tick(:timeout, state)

      %{state |
        voted_for: from,
        timeout: new_timeout
      }
    else
      state
    end
  end

end
