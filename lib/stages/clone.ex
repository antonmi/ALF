defmodule ALF.Clone do
  use ALF.BaseStage

  defstruct [
    name: nil,
    pid: nil,
    to: [],
    subscribe_to: [],
    pipe_module: nil,
    pipeline_module: nil,
    subscribers: []
  ]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:producer_consumer,
      %{state | pid: self()},
      dispatcher: GenStage.BroadcastDispatcher,
      subscribe_to: state.subscribe_to}
  end

  def handle_events([ip], _from, state) do
    ip = %{ip | history: [{state.name, ip.datum} | ip.history]}
    {:noreply, [ip], state}
  end
end
