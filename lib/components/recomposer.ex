defmodule ALF.Components.Recomposer do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :recomposer,
                module: nil,
                function: nil,
                source_code: nil,
                collected_ips: [],
                new_collected_ips: %{}
              ]

  alias ALF.{DSLError, IP, ErrorIP}

  @dsl_options [:name, :opts, :count]

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  @impl true
  def init(state) do
    state = %{
      state
      | pid: self(),
        opts: init_opts(state.module, state.opts),
        source_code: state.source_code || read_source_code(state.module, state.function)
    }

    component_added(state)
    {:producer_consumer, state}
  end

  def init_sync(state, telemetry) do
    %{
      state
      | pid: make_ref(),
        opts: init_opts(state.module, state.opts),
        source_code: state.source_code || read_source_code(state.module, state.function),
        telemetry: telemetry
    }
  end

  @impl true
  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: true} = state) do
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

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: false} = state) do
    case process_ip(ip, state) do
      {nil, state} ->
        {:noreply, [], state}

      {%IP{} = ip, state} ->
        {:noreply, [ip], state}

      {%ErrorIP{}, state} ->
        {:noreply, [], state}
    end
  end

  defp process_ip(current_ip, state) do
    collected_data =
      Enum.map(Map.get(state.new_collected_ips, current_ip.stream_ref, []), & &1.event)

    history = history(current_ip, state)

    case call_function(
           state.module,
           state.function,
           current_ip.event,
           collected_data,
           state.opts
         ) do
      {:ok, :continue} ->
        send_result(current_ip, :destroyed)

        collected = Map.get(state.new_collected_ips, current_ip.stream_ref, []) ++ [current_ip]

        {nil,
         %{
           state
           | new_collected_ips: Map.put(state.new_collected_ips, current_ip.stream_ref, collected)
         }}

      {:ok, {nil, events}} ->
        send_result(current_ip, :destroyed)

        collected =
          Enum.map(events, fn event ->
            build_ip(event, current_ip, history)
          end)

        {nil,
         %{
           state
           | new_collected_ips: Map.put(state.new_collected_ips, current_ip.stream_ref, collected)
         }}

      {:ok, {event, events}} ->
        ip = build_ip(event, current_ip, history)

        send_result(ip, :created_recomposer)

        collected =
          Enum.map(events, fn event ->
            build_ip(event, ip, history)
          end)

        {ip,
         %{
           state
           | new_collected_ips: Map.put(state.new_collected_ips, current_ip.stream_ref, collected)
         }}

      {:ok, event} ->
        ip = build_ip(event, current_ip, history)

        send_result(ip, :created_recomposer)

        {ip,
         %{
           state
           | new_collected_ips: Map.put(state.new_collected_ips, current_ip.stream_ref, [])
         }}

      {:error, error, stacktrace} ->
        error_ip = send_error_result(current_ip, error, stacktrace, state)
        {error_ip, state}
    end
  end

  def sync_process(ip, %__MODULE__{telemetry: false} = state) do
    do_sync_process(ip, state)
  end

  def sync_process(ip, %__MODULE__{telemetry: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        ip = do_sync_process(ip, state)
        {ip, telemetry_data(ip, state)}
      end
    )
  end

  defp do_sync_process(ip, state) do
    collected_ips = get_from_process_dict({state.pid, ip.stream_ref})
    collected_data = Enum.map(collected_ips, & &1.event)
    history = history(ip, state)

    case call_function(
           state.module,
           state.function,
           ip.event,
           collected_data,
           state.opts
         ) do
      {:ok, :continue} ->
        collected_ips = collected_ips ++ [ip]
        put_to_process_dict({state.pid, ip.stream_ref}, collected_ips)
        nil

      {:ok, {nil, events}} ->
        collected =
          Enum.map(events, fn event ->
            build_ip(event, ip, history)
          end)

        put_to_process_dict({state.pid, ip.stream_ref}, collected)
        nil

      {:ok, {event, events}} ->
        ip = build_ip(event, ip, history)

        collected =
          Enum.map(events, fn event ->
            build_ip(event, ip, history)
          end)

        put_to_process_dict({state.pid, ip.stream_ref}, collected)
        ip

      {:ok, event} ->
        put_to_process_dict({state.pid, ip.stream_ref}, [])
        build_ip(event, ip, history)

      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)
    end
  end

  defp build_ip(event, ip, history) do
    %IP{
      stream_ref: ip.stream_ref,
      ref: ip.ref,
      destination: ip.destination,
      init_event: event,
      event: event,
      pipeline_module: ip.pipeline_module,
      recomposed: true,
      debug: ip.debug,
      history: history,
      sync_path: ip.sync_path
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

  defp get_from_process_dict(key), do: Process.get(key, [])

  defp put_to_process_dict(key, ips), do: Process.put(key, ips)
end
