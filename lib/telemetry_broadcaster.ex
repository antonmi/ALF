defmodule ALF.TelemetryBroadcaster do
  use GenServer

  defmodule Handler do
    def handle_event([:alf, :component, type], measurements, metadata, %{pid: pid}) do
      GenServer.cast(pid, {:handle_event, [:alf, :component, type], measurements, metadata})
    end
  end

  defstruct nodes: MapSet.new(),
            pid: nil

  def start_link([]) do
    GenServer.start_link(__MODULE__, %__MODULE__{}, name: __MODULE__)
  end

  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    {:ok, state, {:continue, :attach_telemetry}}
  end

  def handle_continue(:attach_telemetry, %__MODULE__{} = state) do
    do_attach_telemetry(state.pid)
    {:noreply, state}
  end

  @spec register_node(atom(), atom(), atom()) :: {atom(), atom(), atom()}
  def register_node(name, module, function) do
    GenServer.call(__MODULE__, {:register_node, {name, module, function}})
  end

  def nodes(), do: GenServer.call(__MODULE__, :nodes)

  def handle_call({:register_node, {name, module, function}}, _from, state) do
    state = %{state | nodes: MapSet.put(state.nodes, {name, module, function})}
    {:reply, {name, module, function}, state}
  end

  def handle_call(:nodes, _from, state) do
    {:reply, state.nodes, state}
  end

  def handle_cast({:handle_event, [:alf, :component, type], measurements, metadata}, state) do
    state.nodes
    |> Enum.each(fn {name, module, function} ->
      :rpc.call(name, module, function, [[:alf, :component, type], measurements, metadata])
    end)

    {:noreply, state}
  end

  defp do_attach_telemetry(pid) do
    :ok =
      :telemetry.attach_many(
        "telemetry-broadcaster",
        [
          [:alf, :component, :start],
          [:alf, :component, :stop],
          [:alf, :component, :exception]
        ],
        &Handler.handle_event/4,
        %{pid: pid}
      )
  end
end
