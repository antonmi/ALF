defmodule ALF.Components.Broadcaster do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :broadcaster,
                to: []
              ]

  alias ALF.DSLError

  @dsl_options [:count]
  @dsl_requited_options []

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  @impl true
  def init(state) do
    state = %{state | pid: self()}
    component_added(state)

    {:producer_consumer, state, dispatcher: GenStage.BroadcastDispatcher}
  end

  def init_sync(state, telemetry) do
    %{state | pid: make_ref(), telemetry: telemetry}
  end

  @impl true
  def handle_events(
        [%ALF.IP{} = ip],
        _from,
        %__MODULE__{telemetry: true} = state
      ) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        ip = process_ip(ip, state)
        {{:noreply, [ip], state}, telemetry_data(ip, state)}
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry: false} = state) do
    ip = process_ip(ip, state)
    {:noreply, [ip], state}
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
    [%{ip | history: history(ip, state)}]
  end

  defp process_ip(ip, state) do
    Enum.each(2..length(state.subscribers), fn _i ->
      send_result(ip, :composed)
    end)

    %{ip | history: history(ip, state), composed: true}
  end

  def validate_options(name, options) do
    required_left = @dsl_requited_options -- Keyword.keys(options)
    wrong_options = Keyword.keys(options) -- @dsl_options

    unless is_atom(name) do
      raise DSLError, "broadcaster name must be an atom: #{inspect(name)}"
    end

    if Enum.any?(required_left) do
      raise DSLError,
            "Not all the required options are given for the #{name} broadcaster. " <>
              "You forgot specifying #{inspect(required_left)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options for the #{name} broadcaster: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@dsl_options)}"
    end
  end
end
