defmodule ALF.Components.Producer do
  use GenStage
  alias ALF.Manager

  defstruct name: :producer,
            manager_name: nil,
            ips: [],
            pid: nil,
            pipe_module: nil,
            pipeline_module: nil,
            subscribe_to: []

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:producer, %{state | pid: self()}}
  end

  def load_ips(pid, ips) do
    GenServer.cast(pid, ips)
  end

  def handle_demand(_demand, %__MODULE__{ips: [ip | ips], manager_name: manager_name} = state) do
    ip = add_to_in_progress_registry(ip, manager_name)
    state = %{state | ips: ips}
    {:noreply, [ip], state}
  end

  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end

  def handle_cast([new_ip | new_ips], %__MODULE__{ips: ips, manager_name: manager_name} = state) do
    ip = add_to_in_progress_registry(new_ip, manager_name)
    state = %{state | ips: ips ++ new_ips}
    {:noreply, [ip], state}
  end

  def handle_cast([], state) do
    {:noreply, [], state}
  end

  def add_to_in_progress_registry(ip, manager_name) do
    Manager.remove_from_registry(manager_name, [ip], ip.stream_ref)
    ip = %{ip | in_progress: true}
    Manager.add_to_registry(manager_name, [ip], ip.stream_ref)
    ip
  end
end
