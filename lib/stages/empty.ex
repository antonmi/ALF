defmodule ALF.Empty do
  use ALF.BaseStage

  defstruct [
    name: nil,
    pid: nil,
    subscribe_to: [],
    subscribers: [],
    pipe_module: nil,
    pipeline_module: nil,
  ]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:producer_consumer, %{state | pid: self()}, subscribe_to: state.subscribe_to}
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{} = state) do
    ip = %{ip | history: [{state.name, ip.datum} | ip.history]}
    {:noreply, [ip], state}
  end

  def handle_call({:goto, %ALF.IP{} = ip}, _from, %__MODULE__{} = state) do

    ip = %{ip | history: [{state.name, ip.datum} | ip.history]}
    {:reply, :ok, [ip], state}
  end
end
