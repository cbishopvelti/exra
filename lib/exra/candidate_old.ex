defmodule Exra.CandidateOld do

  # We're still catching up with data, so we can't vote.
  def handle_cast({:candidate, _a, _b, _c, _d}, state = %{state: :learner}) do
    {:noreply, state}
  end
  # Check if they're in our cluster
  def handle_cast(message = {:candidate, from, their_term, _b, _c}, state = %{nodes: nodes, term: my_term}) do
    if (from in nodes) do
      handle_cast_2(message, state)
    else
      new_state = vote_for_candidate({from, false, max(their_term, my_term)}, state)
      {:noreply, new_state}
    end
  end
  # Their term is less then we've either already voted, or something else has taken precedence.
  def handle_cast_2({:candidate, from, their_term, _log_term, _log_index}, state = %{term: my_term}) when their_term < my_term do
    new_state = vote_for_candidate({from, false, my_term}, state)
    {:noreply, new_state}
  end
  def handle_cast_2({:candidate, _a, _b, _c, _d}, %{logs: []}) do
    raise "Logs cannot be empty, this should not happen, at the least should contain a genesis log."
  end
  # If we're on the same term, check voted_for
  def handle_cast_2({:candidate, from, term, log_term, log_index}, state = %{term: term, logs: [%{index: my_log_index, term: my_log_term} | _]})
    when (is_nil(state.voted_for) or state.voted_for == from)
  do
    # They have logs that upto or ahead of us, so vote for them.
    if log_term > my_log_term or (log_term == my_log_term and log_index >= my_log_index) do
      new_state = vote_for_candidate({from, true, term}, state)
      {:noreply, new_state}
    else
      # Logs were not fresh enough
      new_state = vote_for_candidate({from, false, term}, state)
      {:noreply, new_state}
    end
  end
  # We're on the same term, and above didn't match, so we've already voted.
  def handle_cast_2({:candidate, from, term, _lt, _li}, state = %{term: term}) do
    new_state = vote_for_candidate({from, false, term}, state)
    {:noreply, new_state}
  end
  # Success
  def handle_cast_2({:candidate, from, their_term, log_term, log_index}, state = %{term: _term, logs: [%{term: my_log_term, index: my_log_index} | _a]})
    when their_term > state.term and (log_term > my_log_term or (log_term == my_log_term and log_index >= my_log_index))
  do
    new_state = vote_for_candidate({from, true, their_term}, state)
    {:noreply, new_state}
  end
  # Invalid reject vote
  def handle_cast_2({:candidate, from, their_term, _lt, _li}, state) do
    new_state = vote_for_candidate({from, false, their_term}, state)
    {:noreply, new_state}
  end

  def vote_for_candidate({from, success, term}, state) do

    GenServer.cast(from, {
      :vote,
      success,
      term
    })
    becoming_follower = term > state.term

    timeout = if (success || becoming_follower) do
      !is_nil(state.timeout) && Process.cancel_timer(state.timeout)
      Exra.tick(:timeout, state)
    else
      state.timeout
    end

    %{state | term: term, timeout: timeout,
      voted_for: (if term > state.term, do: nil, else: state.voted_for) || (if success, do: from, else: nil),
      state: (if becoming_follower, do: :follower, else: state.state)
    }
  end

end
