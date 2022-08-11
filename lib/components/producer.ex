defmodule ALF.Components.Producer do
  use ALF.Components.Basic
  alias ALF.Manager.Streamer

  defstruct Basic.common_attributes() ++
              [
                type: :producer,
                demand: 0,
                manager_name: nil,
                source_code: nil,
                ips: []
              ]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:producer,
     %{
       state
       | pid: self(),
         name: :producer,
         source_code: read_source_code(state.pipeline_module)
     }}
  end

  def load_ips(pid, ips) do
    GenServer.cast(pid, {:load_ips, ips})
  end

  def ips_count(pid), do: GenServer.call(pid, :ips_count)

  def handle_call(:ips_count, _from, state) do
    {:reply, length(state.ips), [], state}
  end

  def handle_demand(1, %__MODULE__{ips: [_ip | _], demand: demand} = state) do
    {ips, new_state} = prepare_state_and_ips(%{state | demand: demand + 1})
    {:noreply, ips, new_state}
  end

  def handle_demand(1, %__MODULE__{ips: [], demand: demand} = state) do
    state = %{state | demand: demand + 1}
    {:noreply, [], state}
  end

  def handle_cast({:load_ips, new_ips}, %__MODULE__{ips: ips, demand: demand} = state) do
    {ips, new_state} = prepare_state_and_ips(%{state | ips: ips ++ new_ips, demand: demand + 0})
    {:noreply, ips, new_state}
  end

  defp prepare_state_and_ips(%__MODULE__{ips: ips, demand: demand} = state) do
    case Enum.split(ips, demand) do
      {[], ips_to_store} ->
        {[], %{state | demand: demand, ips: ips_to_store}}

      {ips_to_send, ips_to_store} ->
        if state.telemetry_enabled do
          send_simple_telemetry_events(ips_to_send, state)
        end

        {ips_to_send, %{state | demand: demand - length(ips_to_send), ips: ips_to_store}}
    end
  end

  defp send_simple_telemetry_events(ips_to_send, state) do
    ips_to_send
    |> Enum.map(fn ip ->
      telemetry_data = telemetry_data(ip, state)
      :telemetry.span([:alf, :component], telemetry_data, fn -> {:ok, telemetry_data} end)
    end)
  end
end
