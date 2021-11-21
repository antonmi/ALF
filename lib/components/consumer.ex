defmodule ALF.Components.Consumer do
  use GenStage

  alias ALF.{ErrorIP, IP, Manager, DoneStatement}

  defstruct name: :consumer,
            manager_name: nil,
            pid: nil,
            pipe_module: nil,
            subscribe_to: [],
            pipeline_module: nil

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:consumer, %{state | pid: self()}, subscribe_to: state.subscribe_to}
  end

  def handle_events([%DoneStatement{ip: ip}], _from, state) do
    Manager.result_ready(state.manager_name, ip)

    {:noreply, [], state}
  end

  def handle_events([ip], _from, state) when is_struct(ip, IP) or is_struct(ip, ErrorIP) do
    Manager.result_ready(state.manager_name, ip)

    {:noreply, [], state}
  end
end
