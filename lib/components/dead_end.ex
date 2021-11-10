defmodule ALF.Components.DeadEnd do
  use ALF.Components.Basic

  defstruct [
    name: nil,
    pid: nil,
    pipe_module: nil,
    pipeline_module: nil,
    subscribe_to: [],
    subscribers: []
  ]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:consumer, %{state | pid: self()}, subscribe_to: state.subscribe_to}
  end

  def handle_events([ip], _from, state) do
    {:noreply, [], state}
  end
end
