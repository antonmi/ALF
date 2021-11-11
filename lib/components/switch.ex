defmodule ALF.Components.Switch do
  use ALF.Components.Basic

  defstruct name: nil,
            pid: nil,
            subscribe_to: [],
            subscribers: [],
            partitions: %{},
            pipe_module: nil,
            pipeline_module: nil,
            cond: nil,
            opts: %{}

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    partitions = Map.keys(state.partitions)

    cond = fn ip ->
      ip = %{ip | history: [{state.name, ip.datum} | ip.history]}

      case call_cond_function(state.cond, ip.datum, state.pipeline_module, state.opts) do
        {:error, error} ->
          {build_error_ip(ip, error, state), hd(partitions)}

        partition ->
          {ip, partition}
      end
    end

    {:producer_consumer, %{state | pid: self()},
     dispatcher: {GenStage.PartitionDispatcher, partitions: partitions, hash: cond},
     subscribe_to: state.subscribe_to}
  end

  def handle_events([ip], _from, state) do
    {:noreply, [ip], state}
  end

  defp call_cond_function(function, datum, pipeline_module, opts) when is_atom(function) do
    apply(pipeline_module, function, [datum, opts])
  rescue
    error ->
      {:error, error}
  end

  defp call_cond_function(cond, datum, _pipeline_module, opts) when is_function(cond) do
    cond.(datum, opts)
  rescue
    error ->
      {:error, error}
  end
end
