defmodule ALF.Components.Clone do
  use ALF.Components.Basic

  defstruct name: nil,
            pid: nil,
            to: [],
            subscribe_to: [],
            pipe_module: nil,
            pipeline_module: nil,
            subscribers: [],
            telemetry_enabled: false

  alias ALF.DSLError

  @dsl_options [:to]
  @dsl_requited_options [:to]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:producer_consumer, %{state | pid: self(), telemetry_enabled: telemetry_enabled?()},
     dispatcher: GenStage.BroadcastDispatcher, subscribe_to: state.subscribe_to}
  end

  def handle_events(
        [%ALF.IP{} = ip],
        _from,
        %__MODULE__{telemetry_enabled: true} = state
      ) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        {:noreply, [ip], state} = do_handle_event(ip, state)
        {{:noreply, [ip], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    do_handle_event(ip, state)
  end

  defp do_handle_event(ip, state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}
    {:noreply, [ip], state}
  end

  def validate_options(name, options) do
    required_left = @dsl_requited_options -- Keyword.keys(options)
    wrong_options = Keyword.keys(options) -- @dsl_options

    unless is_atom(name) do
      raise DSLError, "Clone name must be an atom: #{inspect(name)}"
    end

    if Enum.any?(required_left) do
      raise DSLError,
            "Not all the required options are given for the #{name} clone. " <>
              "You forgot specifying #{inspect(required_left)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options for the #{name} clone: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@dsl_options)}"
    end
  end
end
