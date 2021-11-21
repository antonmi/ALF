defmodule ALF.Components.Producer do
  use GenStage
  alias ALF.Manager

  defstruct name: :producer,
            pid: nil,
            pipe_module: nil,
            pipeline_module: nil,
            subscribe_to: []

  def start_link(_args) do
    GenStage.start_link(__MODULE__, nil)
  end

  def init(_), do: {:producer, []}

  def handle_demand(_demand, [ip | ips]) do
    ip = add_to_in_progress_registry(ip)
    {:noreply, [ip], ips}
  end

  def handle_demand(_demand, []) do
    {:noreply, [], []}
  end

  def load_ips(pid, ips) do
    GenServer.cast(pid, ips)
  end

  def handle_cast([new_ip | new_ips], ips) do
    ip = add_to_in_progress_registry(new_ip)
    {:noreply, [ip], ips ++ new_ips}
  end

  def handle_cast([], ips) do
    {:noreply, [], ips}
  end

  def add_to_in_progress_registry(ip) do
    Manager.remove_from_registry(ip.manager_name, [ip], ip.stream_ref)
    ip = %{ip | in_progress: true}
    Manager.add_to_registry(ip.manager_name, [ip], ip.stream_ref)
    ip
  end
end
