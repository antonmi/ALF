defmodule ALF.Components.GotoPoint do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :goto_point
              ]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:producer_consumer, %{state | pid: self()}, subscribe_to: state.subscribe_to}
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: true} = state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}

    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        ip = %{ip | history: [{state.name, ip.event} | ip.history]}
        {{:noreply, [ip], state}, telemetry_data(ip, state)}
      end
    )
  end

  def sync_process(ip, state) do
    %{ip | history: [{state.name, ip.event} | ip.history]}
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}
    {:noreply, [ip], state}
  end

  def handle_call({:goto, %ALF.IP{} = ip}, _from, %__MODULE__{} = state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}
    {:reply, :ok, [ip], state}
  end
end
