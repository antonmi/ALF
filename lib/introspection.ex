defmodule ALF.Introspection do
  use GenServer

  defstruct pipelines: MapSet.new(),
            pid: nil

  def start_link([]) do
    GenServer.start_link(__MODULE__, %__MODULE__{}, name: __MODULE__)
  end

  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    {:ok, state}
  end

  def add(pipeline), do: GenServer.call(__MODULE__, {:add, pipeline})

  def remove(pipeline), do: GenServer.call(__MODULE__, {:remove, pipeline})

  def pipelines, do: GenServer.call(__MODULE__, :pipelines)

  def info(pipeline), do: GenServer.call(__MODULE__, {:info, pipeline})

  def reset(), do: GenServer.call(__MODULE__, :reset)

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

    # TODO introduce public function
    components =
      ALF.Manager.__state__(pipeline).components
      |> Enum.map(&Map.from_struct/1)

    {:reply, components, state}
  end
end
