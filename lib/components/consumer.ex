defmodule ALF.Components.Consumer do
  use ALF.Components.Basic

  alias ALF.{ErrorIP, IP, Manager.Streamer}

  defstruct Basic.common_attributes() ++
              [
                type: :consumer,
                manager_name: nil
              ]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    state = %{state | pid: self(), name: :consumer}
    {:consumer, state, subscribe_to: state.subscribe_to}
  end

  def handle_events([ip], _from, %__MODULE__{telemetry_enabled: true} = state)
      when is_struct(ip, IP) or is_struct(ip, ErrorIP) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {:noreply, [], state} = do_handle_event(ip, state)
        {{:noreply, [], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([ip], _from, %__MODULE__{telemetry_enabled: false} = state)
      when is_struct(ip, IP) or is_struct(ip, ErrorIP) do
    do_handle_event(ip, state)
  end

  defp do_handle_event(ip, state) do
    Streamer.cast_result_ready(state.manager_name, ip)

    {:noreply, [], state}
  end
end
