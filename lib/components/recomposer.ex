defmodule ALF.Components.Recomposer do
  use ALF.Components.Basic

  defstruct name: nil,
            pid: nil,
            function: nil,
            opts: [],
            subscribe_to: [],
            pipe_module: nil,
            pipeline_module: nil,
            subscribers: [],
            collected_ips: []

  alias ALF.{DSLError, Manager}

  @dsl_options [:function, :opts]
  @dsl_requited_options [:function]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:producer_consumer, %{state | pid: self()}, subscribe_to: state.subscribe_to}
  end

  def handle_events([ip], _from, state) do
    collected_data = Enum.map(state.collected_ips, & &1.datum)

    case call_function(
           state.function,
           ip.datum,
           collected_data,
           state.pipeline_module,
           state.opts
         ) do
      {:ok, :continue} ->
        collected_ips = state.collected_ips ++ [ip]
        {:noreply, [], %{state | collected_ips: collected_ips}}

      {:ok, datum} ->
        Manager.remove_from_registry(ip.manager_name, [ip | state.collected_ips], ip.stream_ref)

        ip =
          build_ip(datum, ip.stream_ref, ip.manager_name, [{state.name, ip.datum} | ip.history])

        Manager.add_to_registry(ip.manager_name, [ip], ip.stream_ref)
        {:noreply, [ip], %{state | collected_ips: []}}

      {:error, error, stacktrace} ->
        {:noreply, [build_error_ip(ip, error, stacktrace, state)], state}
    end
  end

  defp build_ip(datum, stream_ref, manager_name, history) do
    %IP{
      stream_ref: stream_ref,
      ref: make_ref(),
      init_datum: datum,
      datum: datum,
      manager_name: manager_name,
      recomposed: true,
      history: history
    }
  end

  def validate_options(name, options) do
    required_left = @dsl_requited_options -- Keyword.keys(options)
    wrong_options = Keyword.keys(options) -- @dsl_options

    unless is_atom(name) do
      raise DSLError, "Recomposer name must be an atom: #{inspect(name)}"
    end

    if Enum.any?(required_left) do
      raise DSLError,
            "Not all the required options are given for the #{name} recomposer. " <>
              "You forgot specifying #{inspect(required_left)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options for the #{name} recomposer: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@dsl_options)}"
    end
  end

  defp call_function(function, datum, collected_data, pipeline_module, opts)
       when is_atom(function) do
    {:ok, apply(pipeline_module, function, [datum, collected_data, opts])}
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end

  defp call_function(function, datum, collected_data, _pipeline_module, opts)
       when is_function(function) do
    {:ok, function.(datum, collected_data, opts)}
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end
end
