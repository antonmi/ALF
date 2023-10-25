defmodule ALF.Components.Switch do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :switch,
                module: nil,
                function: nil,
                opts: [],
                source_code: nil,
                branches: %{}
              ]

  alias ALF.{DSLError}

  @dsl_options [:branches, :opts, :name]
  @dsl_requited_options [:branches]

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  @impl true
  def init(state) do
    branches = Map.keys(state.branches)

    hash = fn ip ->
      ip = %{ip | history: [{state.name, ip.event} | ip.history]}

      case call_function(state.module, state.function, ip.event, state.opts) do
        {:error, error, stacktrace} ->
          send_error_result(ip, error, stacktrace, state)
          :none

        partition ->
          {ip, partition}
      end
    end

    state = %{
      state
      | pid: self(),
        opts: init_opts(state.module, state.opts),
        source_code: state.source_code || read_source_code(state.module, state.function)
    }

    component_added(state)

    {:producer_consumer, state,
     dispatcher: {GenStage.PartitionDispatcher, partitions: branches, hash: hash}}
  end

  def init_sync(state, telemetry) do
    %{
      state
      | opts: init_opts(state.module, state.opts),
        pid: make_ref(),
        telemetry: telemetry
    }
  end

  @impl true
  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {{:noreply, [ip], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: false} = state) do
    {:noreply, [ip], state}
  end

  def validate_options(name, options) do
    required_left = @dsl_requited_options -- Keyword.keys(options)
    wrong_options = Keyword.keys(options) -- @dsl_options

    unless is_atom(name) do
      raise DSLError, "Switch name must be an atom: #{inspect(name)}"
    end

    if Enum.any?(required_left) do
      raise DSLError,
            "Not all the required options are given for the #{name} switch. " <>
              "You forgot specifying #{inspect(required_left)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options for the #{name} switch: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@dsl_options)}"
    end

    if Enum.count(Keyword.get(options, :branches)) == 0 do
      raise DSLError,
            "There must be at least one branch in the #{name} switch."
    end
  end

  def sync_process(ip, %__MODULE__{telemetry: false} = state) do
    case call_function(state.module, state.function, ip.event, state.opts) do
      {:error, error, stacktrace} ->
        build_error_ip(ip, error, stacktrace, state)

      partition ->
        partition
    end
  end

  def sync_process(ip, %__MODULE__{telemetry: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        case call_function(state.module, state.function, ip.event, state.opts) do
          {:error, error, stacktrace} ->
            {build_error_ip(ip, error, stacktrace, state), telemetry_data(ip, state)}

          partition ->
            {partition, telemetry_data(ip, state)}
        end
      end
    )
  end

  defp call_function(module, function, event, opts) when is_atom(module) and is_atom(function) do
    apply(module, function, [event, opts])
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  catch
    kind, value ->
      {:error, kind, value}
  end

  defp call_function(module, function, event, opts)
       when is_atom(module) and is_function(function) do
    function.(event, opts)
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  catch
    kind, value ->
      {:error, kind, value}
  end
end
