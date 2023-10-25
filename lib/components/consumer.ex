defmodule ALF.Components.Consumer do
  use ALF.Components.Basic

  alias ALF.{ErrorIP, IP}

  defstruct Basic.common_attributes() ++
              [
                type: :consumer
              ]

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  @impl true
  def init(state) do
    state = %{state | pid: self(), name: :consumer}
    component_added(state)
    {:consumer, state}
  end

  def init_sync(state, telemetry_enabled) do
    %{state | pid: make_ref(), name: :consumer, telemetry_enabled: telemetry_enabled}
  end

  @impl true
  def handle_events([ip], _from, %__MODULE__{telemetry_enabled: true} = state)
      when is_struct(ip, IP) or is_struct(ip, ErrorIP) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        send_result(ip, ip)
        {{:noreply, [], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([ip], _from, %__MODULE__{telemetry_enabled: false} = state)
      when is_struct(ip, IP) or is_struct(ip, ErrorIP) do
    send_result(ip, ip)
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
end
