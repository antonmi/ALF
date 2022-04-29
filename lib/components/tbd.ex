defmodule ALF.Components.Tbd do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :tbd
              ]

  alias ALF.DSLError

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    state = %{state | pid: self()}

    {:producer_consumer, state, subscribe_to: state.subscribe_to}
  end

  def handle_events([%IP{} = ip], _from, state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}
    {:noreply, [ip], state}
  end

  def validate_name(atom) do
    unless is_atom(atom) do
      raise DSLError, "Tbd name must be an atom: #{inspect(atom)}"
    end
  end
end
