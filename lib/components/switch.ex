defmodule ALF.Components.Switch do
  use ALF.Components.Basic

  defstruct name: nil,
            pid: nil,
            subscribe_to: [],
            subscribers: [],
            partitions: %{},
            pipe_module: nil,
            pipeline_module: nil,
            cond: nil,
            opts: %{}

  alias ALF.DSLError

  @dsl_options [:partitions, :opts, :cond, :name]
  @dsl_requited_options [:partitions, :cond]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    partitions = Map.keys(state.partitions)

    cond = fn ip ->
      ip = %{ip | history: [{state.name, ip.datum} | ip.history]}

      case call_cond_function(state.cond, ip.datum, state.pipeline_module, state.opts) do
        {:error, error, stacktrace} ->
          {build_error_ip(ip, error, stacktrace, state), hd(partitions)}

        partition ->
          {ip, partition}
      end
    end

    {:producer_consumer, %{state | pid: self()},
     dispatcher: {GenStage.PartitionDispatcher, partitions: partitions, hash: cond},
     subscribe_to: state.subscribe_to}
  end

  def handle_events([ip], _from, state) do
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

  defp call_cond_function(function, datum, pipeline_module, opts) when is_atom(function) do
    apply(pipeline_module, function, [datum, opts])
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end

  defp call_cond_function(cond, datum, _pipeline_module, opts) when is_function(cond) do
    cond.(datum, opts)
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end
end
