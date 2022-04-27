defmodule ALF.Components.DeadEnd do
  use ALF.Components.Basic

  defstruct type: :dead_end,
            name: nil,
            pid: nil,
            pipe_module: nil,
            pipeline_module: nil,
            subscribe_to: [],
            subscribed_to: [],
            subscribers: [],
            telemetry_enabled: false

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:consumer, %{state | pid: self()}, subscribe_to: state.subscribe_to}
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {{:noreply, [], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([%ALF.IP{} = _ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    {:noreply, [], state}
  end
end
