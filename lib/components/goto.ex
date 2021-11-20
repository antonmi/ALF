defmodule ALF.Components.Goto do
  use ALF.Components.Basic

  defstruct name: nil,
            to: nil,
            to_pid: nil,
            function: true,
            opts: [],
            pipe_module: nil,
            pipeline_module: nil,
            pid: nil,
            subscribe_to: [],
            subscribers: []

  alias ALF.Components.GotoPoint

  alias ALF.DSLError

  @dsl_options [:to, :function, :opts]
  @dsl_requited_options [:to, :function]

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

        [_component | _other] = components ->
          raise "Goto component error: found #{Enum.count(components)} components with name #{state.to}"

        [] ->
          raise "Goto component error: no component with name #{state.to}"
      end

    state = %{state | to_pid: pid}
    {:reply, state, [], state}
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{} = state) do
    ip = %{ip | history: [{state.name, ip.datum} | ip.history]}

    case call_condition_function(state.function, ip.datum, state.pipeline_module, state.opts) do
      {:error, error, stacktrace} ->
        {:noreply, [build_error_ip(ip, error, stacktrace, state)], state}

      true ->
        :ok = GenStage.call(state.to_pid, {:goto, ip})
        {:noreply, [], state}

      false ->
        {:noreply, [ip], state}
    end
  end

  def validate_options(name, options) do
    required_left = @dsl_requited_options -- Keyword.keys(options)
    wrong_options = Keyword.keys(options) -- @dsl_options

    unless is_atom(name) do
      raise DSLError, "Goto name must be an atom: #{inspect(name)}"
    end

    if Enum.any?(required_left) do
      raise DSLError,
            "Not all the required options are given for the #{name} goto. " <>
              "You forgot specifying #{inspect(required_left)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options for the #{name} goto: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@dsl_options)}"
    end
  end

  defp call_condition_function(true, _datum, _pipeline_module, _opts), do: true

  defp call_condition_function(function, datum, pipeline_module, opts) when is_atom(function) do
    apply(pipeline_module, function, [datum, opts])
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end

  defp call_condition_function(hash, datum, _pipeline_module, opts) when is_function(hash) do
    hash.(datum, opts)
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end
end
