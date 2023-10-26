defmodule ALF.Components.Clone do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :clone,
                to: []
              ]

  alias ALF.DSLError

  @dsl_options [:to]
  @dsl_requited_options [:to]

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
    [
      %{ip | history: history(ip, state)},
      %{ip | history: history(ip, state)}
    ]
  end

  defp process_ip(ip, state) do
    send_result(ip, :cloned)

    %{ip | history: history(ip, state)}
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
