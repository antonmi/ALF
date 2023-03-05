defmodule ALF.Components.Producer do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :producer,
                demand: 0,
                manager_name: nil,
                source_code: nil,
                ips: []
              ]

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    name = :"#{state.manager_name}.Producer"
    GenStage.start_link(__MODULE__, state, name: name)
  end

  @impl true
  def init(state) do
    {:producer,
     %{
       state
       | pid: self(),
         name: :producer,
         source_code: read_source_code(state.pipeline_module)
     }}
  end

  def init_sync(state, telemetry_enabled) do
    %{
      state
      | pid: make_ref(),
        name: :producer,
        source_code: read_source_code(state.pipeline_module),
        telemetry_enabled: telemetry_enabled
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
    if state.telemetry_enabled do
      send_telemetry_event(ip, state)
    end

    {:noreply, [ip], state}
  end

  def sync_process(ip, %__MODULE__{telemetry_enabled: false}), do: ip

  def sync_process(ip, %__MODULE__{telemetry_enabled: true} = state) do
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
