defmodule ALF.Components.Decomposer do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :decomposer,
                module: nil,
                function: nil,
                opts: [],
                source_code: nil
              ]

  alias ALF.{DSLError, Manager.Streamer}

  @dsl_options [:opts, :name]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    state = %{
      state
      | pid: self(),
        opts: init_opts(state.module, state.opts),
        source_code: read_source_code(state.module, state.function)
    }

    {:producer_consumer, state, subscribe_to: state.subscribe_to}
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        case do_handle_event(ip, state) do
          {:noreply, [], state} = result ->
            {result, telemetry_data(nil, state)}

          {:noreply, ips, state} = result ->
            {result, telemetry_data(ips, state)}
        end
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    do_handle_event(ip, state)
  end

  defp do_handle_event(ip, state) do
    case call_function(state.module, state.function, ip.event, state.opts) do
      {:ok, events} when is_list(events) ->
        Streamer.cast_remove_from_registry(ip.manager_name, [ip], ip.stream_ref)

        ips =
          build_ips(events, ip.stream_ref, ip.manager_name, [{state.name, ip.event} | ip.history])

        Streamer.cast_add_to_registry(ip.manager_name, ips, ip.stream_ref)
        {:noreply, ips, state}

      {:ok, {events, event}} when is_list(events) ->
        ips =
          build_ips(events, ip.stream_ref, ip.manager_name, [{state.name, ip.event} | ip.history])

        ip = %{ip | event: event, history: [{state.name, ip.event} | ip.history]}
        Streamer.cast_add_to_registry(ip.manager_name, ips, ip.stream_ref)
        {:noreply, ips ++ [ip], state}

      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)
        {:noreply, [], state}
    end
  end

  defp build_ips(events, stream_ref, manager_name, history) do
    events
    |> Enum.map(fn event ->
      %IP{
        stream_ref: stream_ref,
        ref: make_ref(),
        init_datum: event,
        event: event,
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

  defp call_function(module, function, event, opts) when is_atom(module) and is_atom(function) do
    {:ok, apply(module, function, [event, opts])}
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  catch
    kind, value ->
      {:error, kind, value}
  end
end
