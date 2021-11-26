defmodule ALF.Components.Decomposer do
  use ALF.Components.Basic

  defstruct name: nil,
            pid: nil,
            module: nil,
            function: nil,
            opts: [],
            subscribe_to: [],
            pipe_module: nil,
            pipeline_module: nil,
            subscribers: [],
            telemetry_enabled: false

  alias ALF.{DSLError, Manager}

  @dsl_options [:opts, :name]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    state = %{
      state
      | pid: self(),
        opts: init_opts(state.module, state.opts),
        telemetry_enabled: telemetry_enabled?()
    }

    {:producer_consumer, state, subscribe_to: state.subscribe_to}
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        case do_handle_event(ip, state) do
          {:noreply, ips, state} = result ->
            {result, telemetry_data(ips, state)}

          {:noreply, [], state} = result ->
            {result, telemetry_data(nil, state)}
        end
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    do_handle_event(ip, state)
  end

  defp do_handle_event(ip, state) do
    case call_function(state.module, state.function, ip.datum, state.opts) do
      {:ok, data} when is_list(data) ->
        Manager.remove_from_registry(ip.manager_name, [ip], ip.stream_ref)

        ips =
          build_ips(data, ip.stream_ref, ip.manager_name, [{state.name, ip.datum} | ip.history])

        Manager.add_to_registry(ip.manager_name, ips, ip.stream_ref)
        {:noreply, ips, state}

      {:ok, {data, datum}} when is_list(data) ->
        ips =
          build_ips(data, ip.stream_ref, ip.manager_name, [{state.name, ip.datum} | ip.history])

        ip = %{ip | datum: datum, history: [{state.name, ip.datum} | ip.history]}
        Manager.add_to_registry(ip.manager_name, ips, ip.stream_ref)
        {:noreply, ips ++ [ip], state}

      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)
        {:noreply, [], state}
    end
  end

  defp build_ips(data, stream_ref, manager_name, history) do
    data
    |> Enum.map(fn datum ->
      %IP{
        stream_ref: stream_ref,
        ref: make_ref(),
        init_datum: datum,
        datum: datum,
        manager_name: manager_name,
        decomposed: true,
        history: history
      }
    end)
  end

  def validate_options(name, options) do
    wrong_options = Keyword.keys(options) -- @dsl_options

    unless is_atom(name) do
      raise DSLError, "Decomposer name must be an atom: #{inspect(name)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options for the #{name} decomposer: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@dsl_options)}"
    end
  end

  defp call_function(module, function, datum, opts) when is_atom(module) and is_atom(function) do
    {:ok, apply(module, function, [datum, opts])}
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end
end
