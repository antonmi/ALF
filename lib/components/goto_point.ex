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

  def init_sync(state, telemetry) do
    %{
      state
      | pid: make_ref(),
        telemetry: telemetry
    }
  end

  @impl true
  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: true} = state) do
    ip = %{ip | history: history(ip, state)}

    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {{:noreply, [ip], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: false} = state) do
    ip = %{ip | history: history(ip, state)}
    {:noreply, [ip], state}
  end

  def handle_call({:goto, %ALF.IP{} = ip}, _from, %__MODULE__{} = state) do
    ip = %{ip | history: history(ip, state)}
    {:reply, :ok, [ip], state}
  end

  def sync_process(ip, %__MODULE__{telemetry: false} = state) do
    %{ip | history: history(ip, state)}
  end

  def sync_process(ip, %__MODULE__{telemetry: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        ip = %{ip | history: history(ip, state)}
        {ip, telemetry_data(ip, state)}
      end
    )
  end
end
