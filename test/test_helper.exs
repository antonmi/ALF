ExUnit.start()

defmodule Helpers do
  def random_atom(prefix) do
    :"#{prefix}_#{Base.encode32(:crypto.strong_rand_bytes(5))}"
  end
end

defmodule ALF.TestProducer do
  use GenStage

  def start_link([]) do
    GenStage.start_link(__MODULE__, [])
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

defmodule ALF.TestConsumer do
  use ALF.Components.Basic

  defstruct subscribe_to: [],
            subscribed_to: [],
            pipeline_module: nil,
            ips: []

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def ips(pid), do: __state__(pid).ips

  def init(state) do
    {:consumer, state, subscribe_to: state.subscribe_to}
  end

  def handle_events([ip], _from, state) do
    state = %{state | ips: state.ips ++ [ip]}
    {:noreply, [], state}
  end

  # for using consumer as the ip destination
  def handle_info({_ref, ip}, state) do
    state = %{state | ips: state.ips ++ [ip]}
    {:noreply, [], state}
  end
end
