defmodule ALF.Components.Clone do
  use ALF.Components.Basic

  defstruct name: nil,
            pid: nil,
            to: [],
            subscribe_to: [],
            pipe_module: nil,
            pipeline_module: nil,
            subscribers: []

  alias ALF.DSLError

  @dsl_options [:to]
  @dsl_requited_options [:to]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    {:producer_consumer, %{state | pid: self()},
     dispatcher: GenStage.BroadcastDispatcher, subscribe_to: state.subscribe_to}
  end

  def handle_events([ip], _from, state) do
    ip = %{ip | history: [{state.name, ip.datum} | ip.history]}
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
