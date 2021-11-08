defmodule ALF.Switch do
  use ALF.BaseStage

  defstruct [
    name: nil,
    pid: nil,
    subscribe_to: [],
    subscribers: [],
    partitions: %{},
    pipe_module: nil,
    pipeline_module: nil,
    hash: nil
  ]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    partitions = Map.keys(state.partitions)
    hash = fn ip ->
      partition = call_hash_function(state.hash, ip.datum, state.pipeline_module)
      ip = %{ip | history: [{state.name, ip.datum} | ip.history]}
      {ip, partition}
    end

    {:producer_consumer,
      %{state | pid: self()},
      dispatcher: {GenStage.PartitionDispatcher, partitions: partitions, hash: hash},
      subscribe_to: state.subscribe_to}
  end

  def handle_events([ip], _from, state) do
    {:noreply, [ip], state}
  end

  defp call_hash_function(function, datum, pipeline_module) when is_atom(function) do
    apply(pipeline_module, function, [datum])
  end

  defp call_hash_function({module, function, _pipeline_module}, datum) when is_atom(module) and is_atom(function) do
    apply(module, function, [datum])
  end

  defp call_hash_function(hash, datum, _pipeline_module) when is_function(hash) do
    hash.(datum)
  end

end
