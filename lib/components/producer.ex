defmodule ALF.Components.Producer do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :producer,
                demand: 0,
                source_code: nil,
                ips: []
              ]

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    name = :"#{state.pipeline_module}.Producer"
    GenStage.start_link(__MODULE__, state, name: name)
  end

  @impl true
  def init(state) do
    state = %{
      state
      | pid: self(),
        name: :producer,
        source_code: state.source_code || read_source_code(state.pipeline_module)
    }

    component_added(state)

    {:producer, state}
  end

  def init_sync(state, telemetry) do
    %{
      state
      | pid: make_ref(),
        name: :producer,
        source_code: state.source_code || read_source_code(state.pipeline_module),
        telemetry: telemetry
    }
  end

  def load_ip(name, ip) do
    GenServer.cast(name, {:load_ip, ip})
  end

  @impl true
  def handle_demand(1, %__MODULE__{ips: [], demand: demand} = state) do
    state = %{state | demand: demand + 1}
    {:noreply, [], state}
  end

  @impl true
  def handle_cast({:load_ip, ip}, state) do
    if state.telemetry do
      send_telemetry_event(ip, state)
    end

    {:noreply, [ip], state}
  end

  def sync_process(ip, %__MODULE__{telemetry: false}), do: ip

  def sync_process(ip, %__MODULE__{telemetry: true} = state) do
    telemetry_data = telemetry_data(ip, state)

    :telemetry.span(
      [:alf, :component],
      telemetry_data,
      fn ->
        {ip, telemetry_data}
      end
    )
  end

  defp send_telemetry_event(ip, state) do
    telemetry_data = telemetry_data(ip, state)
    :telemetry.span([:alf, :component], telemetry_data, fn -> {:ok, telemetry_data} end)
  end
end
