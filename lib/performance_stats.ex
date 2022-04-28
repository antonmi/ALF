defmodule ALF.PerformanceStats do
  use GenServer

  defstruct pid: nil,
            stats: %{}

  defmodule Handler do
    def handle_event([:alf, :component, :stop], measurements, metadata, %{pid: pid}) do
      component = metadata[:component]

      if component.type == :stage do
        duration_micro = div(measurements[:duration], 1000)
        GenServer.cast(pid, {:update_component_stats, component, duration_micro})
      end
    end
  end

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

  @spec stats_for(atom()) :: map()
  def stats_for(pipeline), do: GenServer.call(__MODULE__, {:stats_for, pipeline})

  @spec reset_stats_for(atom()) :: :ok
  def reset_stats_for(pipeline), do: GenServer.call(__MODULE__, {:reset_stats_for, pipeline})

  def handle_call({:stats_for, pipeline}, _from, state) do
    {:reply, Map.get(state.stats, pipeline, nil), state}
  end

  def handle_call({:reset_stats_for, pipeline}, _from, state) do
    stats = Map.delete(state.stats, pipeline)
    {:reply, :ok, %{state | stats: stats}}
  end

  def handle_cast({:update_component_stats, component, duration_micro}, state) do
    stats = state.stats
    pipeline_stats = Map.get(stats, component.pipeline_module, false)

    stats =
      if pipeline_stats do
        stats
      else
        stats
        |> Map.put(component.pipeline_module, %{
          since: DateTime.truncate(DateTime.utc_now(), :microsecond)
        })
      end

    key = component_key(component)
    stats = ensure_key_exists(stats, key)

    component_stats = get_in(stats, key)
    counter = component_stats[:counter] || 0
    sum_time_micro = component_stats[:sum_time_micro] || 0

    stats =
      put_in(stats, key, %{
        counter: counter + 1,
        sum_time_micro: sum_time_micro + duration_micro
      })

    {:noreply, %{state | stats: stats}}
  end

  defp ensure_key_exists(state, []), do: state

  defp ensure_key_exists(stats, [key | keys]) do
    map = Map.get(stats, key)

    if map do
      Map.merge(stats, %{key => ensure_key_exists(map, keys)})
    else
      Map.merge(stats, %{key => ensure_key_exists(%{}, keys)})
    end
  end

  defp component_key(component) do
    [component.pipeline_module, component.stage_set_ref, {component.name, component.number}]
  end

  defp do_attach_telemetry(pid) do
    :ok =
      :telemetry.attach_many(
        "performance-stats",
        [
          [:alf, :component, :stop]
        ],
        &Handler.handle_event/4,
        %{pid: pid}
      )
  end
end
