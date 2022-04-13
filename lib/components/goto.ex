defmodule ALF.Components.Goto do
  use ALF.Components.Basic

  defstruct type: :goto,
            name: nil,
            to: nil,
            to_pid: nil,
            module: nil,
            function: true,
            opts: [],
            pipe_module: nil,
            pipeline_module: nil,
            pid: nil,
            source_code: nil,
            subscribe_to: [],
            subscribers: [],
            telemetry_enabled: false

  alias ALF.Components.GotoPoint

  alias ALF.{DSLError}

  @dsl_options [:to, :opts, :name]
  @dsl_requited_options [:to]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    state = %{
      state
      | pid: self(),
        opts: init_opts(state.module, state.opts),
        source_code: read_source_code(state.module, state.function),
        telemetry_enabled: telemetry_enabled?()
    }

    {:producer_consumer, state, subscribe_to: state.subscribe_to}
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

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        case do_handle_event(ip, state) do
          {:noreply, [ip], state} = result ->
            {result, telemetry_data(ip, state)}

          {:noreply, [], state} = result ->
            {result, telemetry_data(ip, state)}
        end
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    do_handle_event(ip, state)
  end

  defp do_handle_event(ip, state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}

    case call_function(state.module, state.function, ip.event, state.opts) do
      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)
        {:noreply, [], state}

      true ->
        :ok = GenStage.call(state.to_pid, {:goto, ip})
        {:noreply, [], state}

      false ->
        {:noreply, [ip], state}
    end
  end

  def validate_options(atom, options) do
    required_left = @dsl_requited_options -- Keyword.keys(options)
    wrong_options = Keyword.keys(options) -- @dsl_options

    unless is_atom(atom) do
      raise DSLError, "Goto name must be an atom: #{inspect(atom)}"
    end

    if Enum.any?(required_left) do
      raise DSLError,
            "Not all the required options are given for the #{atom} goto. " <>
              "You forgot specifying #{inspect(required_left)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options for the #{atom} goto: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@dsl_options)}"
    end
  end

  defp call_function(_module, true, _event, _opts), do: true

  defp call_function(module, function, event, opts) when is_atom(module) and is_atom(function) do
    apply(module, function, [event, opts])
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end
end
