defmodule ALF.Components.Switch do
  use ALF.Components.Basic

  defstruct type: :switch,
            name: nil,
            pid: nil,
            module: nil,
            function: nil,
            opts: %{},
            source_code: nil,
            subscribe_to: [],
            subscribers: [],
            branches: %{},
            pipe_module: nil,
            pipeline_module: nil,
            telemetry_enabled: false

  alias ALF.{DSLError}

  @dsl_options [:branches, :opts, :name]
  @dsl_requited_options [:branches]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

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
        source_code: read_source_code(state.module, state.function),
        telemetry_enabled: telemetry_enabled?()
    }

    {:producer_consumer, state,
     dispatcher: {GenStage.PartitionDispatcher, partitions: branches, hash: hash},
     subscribe_to: state.subscribe_to}
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {{:noreply, [ip], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
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
