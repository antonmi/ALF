defmodule ALF.Components.Producer do
  use ALF.Components.Basic
  alias ALF.Manager.Streamer

  defstruct type: :producer,
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
    GenServer.cast(pid, ips)
  end

  def ips_count(pid), do: GenServer.call(pid, :ips_count)

  def handle_call(:ips_count, _from, state) do
    {:reply, length(state.ips), [], state}
  end

  def handle_demand(
        _demand,
        %__MODULE__{ips: [ip | ips], manager_name: manager_name, telemetry_enabled: true} = state
      ) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {:noreply, [ip], state} = do_handle_demand(ip, ips, manager_name, state)
        {{:noreply, [ip], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_demand(
        _demand,
        %__MODULE__{ips: [ip | ips], manager_name: manager_name, telemetry_enabled: false} = state
      ) do
    do_handle_demand(ip, ips, manager_name, state)
  end

  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end

  defp do_handle_demand(ip, ips, manager_name, state) do
    ip = add_to_in_progress_registry(ip, manager_name)
    state = %{state | ips: ips}
    {:noreply, [ip], state}
  end

  def handle_cast(
        [new_ip | new_ips],
        %__MODULE__{ips: ips, manager_name: manager_name, telemetry_enabled: true} = state
      ) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(new_ip, state),
      fn ->
        {:noreply, [ip], state} = do_handle_cast([new_ip | new_ips], ips, manager_name, state)
        {{:noreply, [ip], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_cast(
        [new_ip | new_ips],
        %__MODULE__{ips: ips, manager_name: manager_name, telemetry_enabled: false} = state
      ) do
    do_handle_cast([new_ip | new_ips], ips, manager_name, state)
  end

  def handle_cast([], state) do
    {:noreply, [], state}
  end

  def handle_subscribe(:consumer, subscription_options, from, state) do
    subscribers = [from | state.subscribers]
    {:automatic, %{state | subscribers: subscribers}}
  end

  defp do_handle_cast([new_ip | new_ips], ips, manager_name, state) do
    ip = add_to_in_progress_registry(new_ip, manager_name)
    state = %{state | ips: ips ++ new_ips}
    {:noreply, [ip], state}
  end

  def add_to_in_progress_registry(ip, manager_name) do
    Streamer.cast_remove_from_registry(manager_name, [ip], ip.stream_ref)
    ip = %{ip | in_progress: true}
    Streamer.cast_add_to_registry(manager_name, [ip], ip.stream_ref)
    ip
  end
end
