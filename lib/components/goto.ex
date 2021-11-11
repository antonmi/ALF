defmodule ALF.Components.Goto do
  use ALF.Components.Basic

  defstruct name: nil,
            to: nil,
            to_pid: nil,
            if: true,
            opts: %{},
            pipe_module: nil,
            pipeline_module: nil,
            pid: nil,
            subscribe_to: [],
            subscribers: []

  alias ALF.Components.GotoPoint

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:producer_consumer, %{state | pid: self()}, subscribe_to: state.subscribe_to}
  end

  def find_where_to_go(pid, components) do
    GenStage.call(pid, {:find_where_to_go, components})
  end

  def handle_call({:find_where_to_go, components}, _from, state) do
    pid =
      case Enum.filter(components, &(&1.name == state.to and &1.__struct__ == GotoPoint)) do
        [component] ->
          component.pid

        [component | _other] = components ->
          raise "Goto component error: found #{Enum.count(components)} components with name #{state.to}"

        [] ->
          raise "Goto component error: no component with name #{state.to}"
      end

    state = %{state | to_pid: pid}
    {:reply, state, [], state}
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{} = state) do
    ip = %{ip | history: [{state.name, ip.datum} | ip.history]}

    case call_condition_function(state.if, ip.datum, state.pipeline_module, state.opts) do
      {:error, error} ->
        {:noreply, [build_error_ip(ip, error, state)], state}

      true ->
        :ok = GenStage.call(state.to_pid, {:goto, ip})
        {:noreply, [], state}

      false ->
        {:noreply, [ip], state}
    end
  end

  defp call_condition_function(function, datum, pipeline_module, opts) when is_atom(function) do
    apply(pipeline_module, function, [datum, opts])
  rescue
    error ->
      {:error, error}
  end

  defp call_condition_function(hash, datum, _pipeline_module, opts) when is_function(hash) do
    hash.(datum, opts)
  rescue
    error ->
      {:error, error}
  end
end
