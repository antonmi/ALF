defmodule ALF.Components.Composer do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :composer,
                module: nil,
                function: nil,
                source_code: nil,
                memo: nil,
                stream_memos: %{},
                collected_ips: [],
                new_collected_ips: %{}
              ]

  alias ALF.{DSLError, IP}

  @dsl_options [:name, :count, :opts, :memo]

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
        {ips, state} = process_ip(ip, state)
        {{:noreply, ips, state}, telemetry_data(ips, state)}
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: false} = state) do
    {ips, state} = process_ip(ip, state)
    {:noreply, ips, state}
  end

  defp process_ip(current_ip, state) do
    history = history(current_ip, state)
    memo = Map.get(state.stream_memos, current_ip.stream_ref, state.memo)

    case call_function(state.module, state.function, current_ip.event, memo, state.opts) do
      {:ok, {events, memo}} when is_list(events) ->
        ips =
          Enum.map(events, fn event ->
            ip = build_ip(event, current_ip, history)
            send_result(ip, :composed)
            ip
          end)

        send_result(current_ip, :destroyed)
        stream_memos = Map.put(state.stream_memos, current_ip.stream_ref, memo)
        {ips, %{state | stream_memos: stream_memos}}

      {:error, error, stacktrace} ->
        send_error_result(current_ip, error, stacktrace, state)
        {[], state}

      {:ok, other} ->
        error =
          "Composer \"#{state.name}\" must return the {[event], memo} tuple. Got #{inspect(other)}"

        send_error_result(current_ip, error, [], state)
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
    memo = get_from_process_dict({state.pid, ip.stream_ref}) || state.memo
    history = history(ip, state)

    case call_function(state.module, state.function, ip.event, memo, state.opts) do
      {:ok, {events, memo}} when is_list(events) ->
        put_to_process_dict({state.pid, ip.stream_ref}, memo)

        case Enum.map(events, &build_ip(&1, ip, history)) do
          [] -> nil
          ips -> ips
        end

      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)

      {:ok, other} ->
        error =
          "Composer \"#{state.name}\" must return the {[event], memo} tuple. Got #{inspect(other)}"

        send_error_result(ip, error, [], state)
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
      composed: true,
      debug: ip.debug,
      history: history,
      sync_path: ip.sync_path,
      plugs: ip.plugs
    }
  end

  def validate_options(name, options) do
    wrong_options = Keyword.keys(options) -- @dsl_options

    unless is_atom(name) do
      raise DSLError, "Composer name must be an atom: #{inspect(name)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options for the #{name} composer: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@dsl_options)}"
    end
  end

  defp call_function(module, function, event, memo, opts)
       when is_atom(module) and is_atom(function) do
    {:ok, apply(module, function, [event, memo, opts])}
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  catch
    kind, value ->
      {:error, kind, value}
  end

  defp get_from_process_dict(key), do: Process.get(key, nil)

  defp put_to_process_dict(key, memo), do: Process.put(key, memo)
end
