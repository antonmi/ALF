defmodule ALF.Introspection do
  use GenServer

  defstruct pipelines: MapSet.new(),
            pid: nil

  alias ALF.PerformanceStats

  def start_link([]) do
    GenServer.start_link(__MODULE__, %__MODULE__{}, name: __MODULE__)
  end

  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    {:ok, state}
  end

  @spec add(atom()) :: atom()
  def add(pipeline) when is_atom(pipeline) do
    GenServer.call(__MODULE__, {:add, pipeline})
  end

  @spec remove(atom()) :: atom()
  def remove(pipeline) when is_atom(pipeline) do
    GenServer.call(__MODULE__, {:remove, pipeline})
  end

  @spec pipelines() :: MapSet.t()
  def pipelines, do: GenServer.call(__MODULE__, :pipelines)

  @spec info(atom) :: list(map())
  def info(pipeline) when is_atom(pipeline) do
    GenServer.call(__MODULE__, {:info, pipeline})
  end

  @spec reset() :: :ok
  def reset(), do: GenServer.call(__MODULE__, :reset)

  @spec performance_stats(atom) :: map()
  def performance_stats(pipeline) when is_atom(pipeline) do
    GenServer.call(__MODULE__, {:performance_stats, pipeline})
  end

  def handle_call({:add, pipeline}, _from, state) do
    state = %{state | pipelines: MapSet.put(state.pipelines, pipeline)}
    {:reply, pipeline, state}
  end

  def handle_call({:remove, pipeline}, _from, state) do
    state = %{state | pipelines: MapSet.delete(state.pipelines, pipeline)}
    {:reply, pipeline, state}
  end

  def handle_call(:pipelines, _from, state) do
    {:reply, state.pipelines, state}
  end

  def handle_call(:reset, _from, state) do
    state = %{state | pipelines: MapSet.new()}
    {:reply, :ok, state}
  end

  def handle_call({:info, pipeline}, _from, state) do
    unless MapSet.member?(state.pipelines, pipeline) do
      raise "No such pipeline: #{pipeline}"
    end

    components =
      pipeline
      |> ALF.Manager.reload_components_states()
      |> Enum.map(&Map.from_struct/1)

    {:reply, components, state}
  end

  def handle_call({:performance_stats, pipeline}, _from, state) do
    stats = PerformanceStats.stats_for(pipeline)
    {:reply, stats, state}
  end
end
