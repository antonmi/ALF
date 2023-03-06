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
    {:consumer, %{state | pid: self()}, subscribe_to: state.subscribe_to}
  end

  def init_sync(state, telemetry_enabled) do
    %{state | pid: make_ref(), telemetry_enabled: telemetry_enabled}
  end

  @impl true
  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        send_result(ip, :destroyed)

        {{:noreply, [], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    send_result(ip, :destroyed)

    {:noreply, [], state}
  end

  def sync_process(_ip, %__MODULE__{telemetry_enabled: false}) do
    nil
  end

  def sync_process(ip, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {nil, telemetry_data(ip, state)}
      end
    )
  end
end
