defmodule ALF.Loop do
  use ALF.BaseStage

  defstruct [
    name: nil,
    to: nil,
    to_pid: nil,
    if: true,
    pipe_module: nil,
    pipeline_module: nil,
    pid: nil,
    subscribe_to: [],
    subscribers: []
  ]


  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:producer_consumer, %{state | pid: self()}, subscribe_to: state.subscribe_to}
  end

  def find_where_to_loop(pid, stages) do
    GenStage.call(pid, {:find_where_to_loop, stages})
  end

  def handle_call({:find_where_to_loop, stages}, _from, state) do
    pid = case Enum.filter(stages, &(&1.name == state.to and &1.__struct__ == ALF.Empty)) do
      [stage] ->
        stage.pid
      [stage | _other] = stages ->
#        raise "Loop state error: found #{Enum.count(stages)} stages with name #{state.to}"
        1
      nil ->
#        raise "Loop state error: no stage with name #{state.to}"
1
    end

    state = %{state | to_pid: pid}
    {:reply, state, [], state}
  end



  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{} = state) do
    ip = %{ip | history: [{state.name, ip.datum} | ip.history]}

    if (state.if === true) or call_condition_function(state.if, ip.datum, state.pipeline_module) do
      :ok = GenStage.call(state.to_pid, {:loop, ip})
      {:noreply, [], state}
    else
      {:noreply, [ip], state}
    end
  end

  defp call_condition_function(function, datum, pipeline_module) when is_atom(function) do
    apply(pipeline_module, function, [datum])
  end

  defp call_condition_function({module, function, _pipeline_module}, datum) when is_atom(module) and is_atom(function) do
    apply(module, function, [datum])
  end

  defp call_condition_function(hash, datum, _pipeline_module) when is_function(hash) do
    hash.(datum)
  end
end
