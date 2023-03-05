defmodule ALF.Components.Stage do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :stage,
                module: nil,
                function: nil,
                count: 1,
                number: 0,
                stage_set_ref: nil,
                opts: [],
                source_code: nil
              ]

  alias ALF.{DoneStatement, DSLError}
  @dsl_options [:opts, :count, :name]

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
        source_code: read_source_code(state.module, state.function),
        subscribers: []
    }

    {:producer_consumer, state, subscribe_to: state.subscribe_to}
  end

  def init_sync(state, telemetry_enabled) do
    ref = make_ref()

    %{
      state
      | pid: ref,
        stage_set_ref: ref,
        opts: init_opts(state.module, state.opts),
        source_code: read_source_code(state.module, state.function),
        telemetry_enabled: telemetry_enabled
    }
  end

  def inc_count(%__MODULE__{pid: pid}), do: GenStage.call(pid, :inc_count)
  def dec_count(%__MODULE__{pid: pid}), do: GenStage.call(pid, :dec_count)

  @impl true
  def handle_call(:inc_count, _from, state) do
    state = %{state | count: state.count + 1}
    {:reply, state, [], state}
  end

  def handle_call(:dec_count, _from, state) do
    state = %{state | count: state.count - 1}
    {:reply, state, [], state}
  end

  @impl true
  def handle_events([%IP{} = ip], _from, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        case process_ip(ip, state) do
          %IP{done!: false} = ip ->
            {{:noreply, [ip], state}, telemetry_data(ip, state)}

          %IP{done!: true} = ip ->
            {{:noreply, [], state}, telemetry_data(ip, state)}

          %ErrorIP{} = ip ->
            {{:noreply, [], state}, telemetry_data(ip, state)}
        end
      end
    )
  end

  def handle_events([%IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    case process_ip(ip, state) do
      %IP{done!: false} = ip ->
        {:noreply, [ip], state}

      %IP{done!: true} ->
        {:noreply, [], state}

      %ErrorIP{} ->
        {:noreply, [], state}
    end
  end

  def validate_options(atom, options) do
    wrong_options = Keyword.keys(options) -- @dsl_options

    unless is_atom(atom) do
      raise DSLError, "Stage must be an atom: #{inspect(atom)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options for the #{atom} stage: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@dsl_options)}"
    end
  end

  defp process_ip(ip, state) do
    ip = %{ip | history: [{{state.name, state.number}, ip.event} | ip.history]}

    case try_apply(ip.event, {state.module, state.function, state.opts}) do
      {:ok, new_event} ->
        %{ip | event: new_event}

      {:error, %DoneStatement{event: event}, _stacktrace} ->
        ip = %{ip | event: event, done!: true}
        send_result(ip, ip)

      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)
    end
  end

  def sync_process(ip, %__MODULE__{telemetry_enabled: false} = state) do
    do_sync_process(ip, state)
  end

  def sync_process(ip, %__MODULE__{telemetry_enabled: true} = state) do
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
    case try_apply(ip.event, {state.module, state.function, state.opts}) do
      {:ok, new_event} ->
        %{ip | event: new_event}

      {:error, %DoneStatement{event: event}, _stacktrace} ->
        %{ip | event: event, done!: true}

      {:error, error, stacktrace} ->
        build_error_ip(ip, error, stacktrace, state)
    end
  end

  defp try_apply(event, {module, function, opts}) do
    new_datum = apply(module, function, [event, opts])
    {:ok, new_datum}
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  catch
    kind, value ->
      {:error, kind, value}
  end
end
