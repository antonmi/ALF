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

  def init_sync(state, telemetry_enabled) do
    %{state | pid: make_ref(), name: :consumer, telemetry_enabled: telemetry_enabled}
  end

  def handle_events([ip], _from, %__MODULE__{telemetry_enabled: true} = state)
      when is_struct(ip, IP) or is_struct(ip, ErrorIP) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        ip = send_result(ip, state)
        {{:noreply, [], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([ip], _from, %__MODULE__{telemetry_enabled: false} = state)
      when is_struct(ip, IP) or is_struct(ip, ErrorIP) do
    send_result(ip, state)
    {:noreply, [], state}
  end

  def sync_process(ip, %__MODULE__{telemetry_enabled: false}) do
    ip
  end

  def sync_process(ip, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {ip, telemetry_data(ip, state)}
      end
    )
  end

  defp send_result(ip, state) do
    if ip.stream_ref do
      # old code
      raise "shouldn't be there"
      Streamer.cast_result_ready(state.manager_name, ip)
      ip
    else
      if ip.new_stream_ref do
        send(ip.destination, {ip.new_stream_ref, ip})
      else
        send(ip.destination, {ip.ref, ip})
      end
    end

    ip
  end
end
