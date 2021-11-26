defmodule ALF.Components.Consumer do
  use GenStage

  import ALF.Components.Basic, only: [telemetry_enabled?: 0, telemetry_data: 2]

  alias ALF.{ErrorIP, IP, Manager}

  defstruct name: :consumer,
            manager_name: nil,
            pid: nil,
            pipe_module: nil,
            subscribe_to: [],
            pipeline_module: nil,
            telemetry_enabled: false

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:consumer, %{state | pid: self(), telemetry_enabled: telemetry_enabled?()},
     subscribe_to: state.subscribe_to}
  end

  def handle_events([ip], _from, %__MODULE__{telemetry_enabled: true} = state)
      when is_struct(ip, IP) or is_struct(ip, ErrorIP) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {:noreply, [], state} = do_handle_event(ip, state)
        {{:noreply, [], state}, telemetry_data(nil, state)}
      end
    )
  end

  def handle_events([ip], _from, %__MODULE__{telemetry_enabled: false} = state)
      when is_struct(ip, IP) or is_struct(ip, ErrorIP) do
    do_handle_event(ip, state)
  end

  defp do_handle_event(ip, state) do
    Manager.result_ready(state.manager_name, ip)

    {:noreply, [], state}
  end
end
