defmodule ALF.Components.Recomposer do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :recomposer,
                module: nil,
                function: nil,
                opts: [],
                source_code: nil,
                collected_ips: []
              ]

  alias ALF.{DSLError, IP, ErrorIP, Manager.Streamer}

  @dsl_options [:name, :opts]

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
        case process_ip(ip, state) do
          {nil, state} ->
            {{:noreply, [], state}, telemetry_data(nil, state)}

          {%IP{} = ip, state} ->
            {{:noreply, [ip], state}, telemetry_data(ip, state)}

          {%ErrorIP{} = error_ip, state} ->
            {{:noreply, [], state}, telemetry_data(error_ip, state)}
        end
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    case process_ip(ip, state) do
      {nil, state} ->
        {:noreply, [], state}

      {%IP{} = ip, state} ->
        {:noreply, [ip], state}

      {%ErrorIP{}, state} ->
        {:noreply, [], state}
    end
  end

  defp process_ip(ip, state) do
    collected_data = Enum.map(state.collected_ips, & &1.event)

    case call_function(
           state.module,
           state.function,
           ip.event,
           collected_data,
           state.opts
         ) do
      {:ok, :continue} ->
        collected_ips = state.collected_ips ++ [ip]
        {nil, %{state | collected_ips: collected_ips}}

      {:ok, {event, events}} ->
        Streamer.cast_remove_from_registry(
          ip.manager_name,
          [ip | state.collected_ips],
          ip.stream_ref
        )

        ip =
          build_ip(event, ip.stream_ref, ip.manager_name, [{state.name, ip.event} | ip.history])

        collected =
          Enum.map(events, fn event ->
            build_ip(event, ip.stream_ref, ip.manager_name, [{state.name, ip.event} | ip.history])
          end)

        Streamer.cast_add_to_registry(ip.manager_name, [ip], ip.stream_ref)
        {ip, %{state | collected_ips: collected}}

      {:ok, event} ->
        Streamer.cast_remove_from_registry(
          ip.manager_name,
          [ip | state.collected_ips],
          ip.stream_ref
        )

        ip =
          build_ip(event, ip.stream_ref, ip.manager_name, [{state.name, ip.event} | ip.history])

        Streamer.cast_add_to_registry(ip.manager_name, [ip], ip.stream_ref)
        {ip, %{state | collected_ips: []}}

      {:error, error, stacktrace} ->
        error_ip = send_error_result(ip, error, stacktrace, state)
        {error_ip, state}
    end
  end

  defp build_ip(event, stream_ref, manager_name, history) do
    %IP{
      stream_ref: stream_ref,
      ref: make_ref(),
      init_datum: event,
      event: event,
      manager_name: manager_name,
      recomposed: true,
      history: history
    }
  end

  def validate_options(name, options) do
    wrong_options = Keyword.keys(options) -- @dsl_options

    unless is_atom(name) do
      raise DSLError, "Recomposer name must be an atom: #{inspect(name)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options for the #{name} recomposer: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@dsl_options)}"
    end
  end

  defp call_function(module, function, event, collected_data, opts)
       when is_atom(module) and is_atom(function) do
    {:ok, apply(module, function, [event, collected_data, opts])}
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  catch
    kind, value ->
      {:error, kind, value}
  end
end
