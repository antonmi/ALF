defmodule ALF.Components.GotoPoint do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :goto_point
              ]

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  @impl true
  def init(state) do
    state = %{state | pid: self()}
    component_added(state)
    {:producer_consumer, state}
  end

  def init_sync(state, telemetry_enabled) do
    %{
      state
      | pid: make_ref(),
        telemetry_enabled: telemetry_enabled
    }
  end

  @impl true
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

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}
    {:noreply, [ip], state}
  end

  def handle_call({:goto, %ALF.IP{} = ip}, _from, %__MODULE__{} = state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}
    {:reply, :ok, [ip], state}
  end

  def sync_process(ip, %__MODULE__{telemetry_enabled: false} = state) do
    %{ip | history: [{state.name, ip.event} | ip.history]}
  end

  def sync_process(ip, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        ip = %{ip | history: [{state.name, ip.event} | ip.history]}
        {ip, telemetry_data(ip, state)}
      end
    )
  end
end
