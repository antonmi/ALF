defmodule ALF.Components.Tbd do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :tbd
              ]

  alias ALF.DSLError

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  @impl true
  def init(state) do
    state = %{state | pid: self()}

    {:producer_consumer, state, subscribe_to: state.subscribe_to}
  end

  def init_sync(state, _telemetry_enabled) do
    %{state | pid: make_ref()}
  end

  @impl true
  def handle_events([%IP{} = ip], _from, state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}
    {:noreply, [ip], state}
  end

  def sync_process(ip, state) do
    %{ip | history: [{state.name, ip.event} | ip.history]}
  end

  def validate_name(atom) do
    unless is_atom(atom) do
      raise DSLError, "Tbd name must be an atom: #{inspect(atom)}"
    end
  end
end
