defmodule ALF.Components.DeadEnd do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :dead_end
              ]

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  @impl true
  def init(state) do
    state = %{state | pid: self()}
    component_added(state)
    {:consumer, state}
  end

  def init_sync(state, telemetry) do
    %{state | pid: make_ref(), telemetry: telemetry}
  end

  @impl true
  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        send_result(ip, :destroyed)

        {{:noreply, [], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: false} = state) do
    send_result(ip, :destroyed)

    {:noreply, [], state}
  end

  def sync_process(_ip, %__MODULE__{telemetry: false}) do
    nil
  end

  def sync_process(ip, %__MODULE__{telemetry: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {nil, telemetry_data(ip, state)}
      end
    )
  end
end
