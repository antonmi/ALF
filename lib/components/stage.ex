defmodule ALF.Components.Stage do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :stage,
                module: nil,
                function: nil,
                count: 1,
                number: 0,
                opts: [],
                source_code: nil
              ]

  alias ALF.DSLError
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
        source_code: state.source_code || read_source_code(state.module, state.function),
        subscribers: []
    }

    component_added(state)
    {:producer_consumer, state}
  end

  def init_sync(state, telemetry) do
    ref = make_ref()

    %{
      state
      | pid: ref,
        stage_set_ref: ref,
        opts: init_opts(state.module, state.opts),
        source_code: state.source_code || read_source_code(state.module, state.function),
        telemetry: telemetry
    }
  end

  @impl true
  def handle_events([%IP{} = ip], _from, %__MODULE__{telemetry: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        case process_ip(ip, state) do
          %IP{} = ip ->
            {{:noreply, [ip], state}, telemetry_data(ip, state)}

          %ErrorIP{} = ip ->
            {{:noreply, [], state}, telemetry_data(ip, state)}
        end
      end
    )
  end

  def handle_events([%IP{} = ip], _from, %__MODULE__{telemetry: false} = state) do
    case process_ip(ip, state) do
      %IP{} = ip ->
        {:noreply, [ip], state}

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

      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)
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
    case try_apply(ip.event, {state.module, state.function, state.opts}) do
      {:ok, new_event} ->
        %{ip | event: new_event}

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
