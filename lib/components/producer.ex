defmodule ALF.Components.Producer do
  use ALF.Components.Basic
  alias ALF.Manager.Streamer

  defstruct type: :producer,
            demand: 0,
            name: :producer,
            manager_name: nil,
            ips: [],
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
    {:producer, %{state | pid: self(), telemetry_enabled: telemetry_enabled?()}}
  end

  def load_ips(pid, ips) do
    GenServer.cast(pid, {:load_ips, ips})
  end

  def ips_count(pid), do: GenServer.call(pid, :ips_count)

  def handle_call(:ips_count, _from, state) do
    {:reply, length(state.ips), [], state}
  end

  def send_extra_event(pid) do
    Process.send_after(pid, :send_extra_event, 1)
  end

  def handle_info(:send_extra_event, state) do
    IO.inspect("111111111111111111111111111111111111111111111111111111")
    IO.inspect("111111111111111111111111111111111111111111111111111111")
    IO.inspect("111111111111111111111111111111111111111111111111111111")
    IO.inspect("111111111111111111111111111111111111111111111111111111")
    IO.inspect(state.ips)
    case state.ips do
      [] ->
        {:noreply, [], state}
      [ip | _] ->
        {:noreply, [ip], state}
    end
  end

  def handle_demand(
        1,
        %__MODULE__{ips: [ip | ips], manager_name: manager_name, telemetry_enabled: true} = state
      ) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {:noreply, [ip], state} = send_ip(ip, ips, manager_name, state)
        {{:noreply, [ip], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_demand(
        1,
        %__MODULE__{ips: [ip | ips], manager_name: manager_name, telemetry_enabled: false} = state
      ) do
    send_ip([ip], ips, manager_name, state)
  end

  def handle_demand(1, %__MODULE__{ips: [], demand: demand} = state) do
    state = %{state | demand: state.demand + 1}
    {:noreply, [], state}
  end

  def handle_cast(
        {:load_ips, [new_ip | new_ips]},
        %__MODULE__{ips: ips, manager_name: manager_name, telemetry_enabled: true} = state
      ) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(new_ip, state),
      fn ->
        {:noreply, [ip], state} = send_ip([new_ip | new_ips], ips, manager_name, state)
        {{:noreply, [ip], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_cast(
        {:load_ips, [new_ip | new_ips]},
        %__MODULE__{ips: ips, manager_name: manager_name, telemetry_enabled: false} = state
      ) do
    send_ip([new_ip | new_ips], ips, manager_name, state)
  end

  def handle_cast({:load_ips, []}, state) do
    {:noreply, [], state}
  end

  defp send_ip(new_ips, ips, manager_name, state) do
    [ip_to_send | ips_to_store] = new_ips ++ ips
    ip_to_send = add_to_in_progress_registry(ip_to_send, manager_name)
    state = %{state | ips: ips_to_store}
    {:noreply, [ip_to_send], state}
  end


  def add_to_in_progress_registry(ip, manager_name) do
    Streamer.cast_remove_from_registry(manager_name, [ip], ip.stream_ref)
    ip = %{ip | in_progress: true}
    Streamer.cast_add_to_registry(manager_name, [ip], ip.stream_ref)
    ip
  end
end
