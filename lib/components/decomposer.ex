defmodule ALF.Components.Decomposer do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :decomposer,
                module: nil,
                function: nil,
                source_code: nil
              ]

  alias ALF.DSLError
  @dsl_options [:opts, :name, :count]

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

  @spec init_sync(t(), boolean) :: t()
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
          {[], state} ->
            {{:noreply, [], state}, telemetry_data(nil, state)}

          {ips, state} ->
            {{:noreply, ips, state}, telemetry_data(ips, state)}
        end
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: false} = state) do
    case process_ip(ip, state) do
      {[], state} ->
        {:noreply, [], state}

      {ips, state} ->
        {:noreply, ips, state}
    end
  end

  defp process_ip(ip, state) do
    case call_function(state.module, state.function, ip.event, state.opts) do
      {:ok, events} when is_list(events) ->
        ips = build_ips(events, ip, history(ip, state))

        Enum.each(ips, &send_result(&1, :created_decomposer))
        send_result(ip, :destroyed)

        {ips, state}

      {:ok, {events, event}} when is_list(events) ->
        ips = build_ips(events, ip, history(ip, state))

        Enum.each(ips, &send_result(&1, :created_decomposer))

        ip = %{ip | event: event, history: history(ip, state)}
        {ips ++ [ip], state}

      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)
        {[], state}
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
        ips = do_sync_process(ip, state)
        {ips, telemetry_data(ips, state)}
      end
    )
  end

  defp do_sync_process(ip, state) do
    case call_function(state.module, state.function, ip.event, state.opts) do
      {:ok, events} when is_list(events) ->
        build_ips(events, ip, history(ip, state))

      {:ok, {events, event}} when is_list(events) ->
        ips = build_ips(events, ip, history(ip, state))

        ip = %{ip | event: event, history: history(ip, state)}
        [ip | ips]

      {:error, error, stacktrace} ->
        build_error_ip(ip, error, stacktrace, state)
    end
  end

  defp build_ips(events, ip, history) do
    events
    |> Enum.map(fn event ->
      %IP{
        stream_ref: ip.stream_ref,
        destination: ip.destination,
        ref: ip.ref,
        init_event: event,
        event: event,
        pipeline_module: ip.pipeline_module,
        decomposed: true,
        debug: ip.debug,
        history: history,
        sync_path: ip.sync_path
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
