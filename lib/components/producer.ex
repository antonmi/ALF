defmodule ALF.Components.Producer do
  use GenStage

  defstruct name: :producer,
            pid: nil,
            pipe_module: nil,
            pipeline_module: nil,
            subscribe_to: []

  def start_link(args) do
    GenStage.start_link(__MODULE__, nil)
  end

  def init(_), do: {:producer, []}

  def handle_demand(_demand, [ip | ips]) do
    {:noreply, [ip], ips}
  end

  def handle_demand(_demand, []) do
    {:noreply, [], []}
  end

  def handle_cast([new_ip | new_ips], ips) do
    {:noreply, [new_ip], ips ++ new_ips}
  end
end
