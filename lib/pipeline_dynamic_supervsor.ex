defmodule ALF.PipelineDynamicSupervisor do
  use DynamicSupervisor

  def start_link(state) do
    DynamicSupervisor.start_link(__MODULE__, state, name: state[:name])
  end

  @impl true
  def init(_state) do
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 3, max_seconds: 5)
  end
end
