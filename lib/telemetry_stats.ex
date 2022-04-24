defmodule ALF.TelemetryStats do
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

  def stats_for(pipeline), do: GenServer.call(__MODULE__, {:stats_for, pipeline})

  def handle_call({:stats_for, pipeline}, _from, state) do
    {:reply, Map.get(state.stats, pipeline, nil), state}
  end

  def handle_cast({:update_component_stats, component, duration_micro}, state) do
    pipeline_stats = Map.get(state.stats, component.pipeline_module, false)

    stats =
      if pipeline_stats do
        state.stats
      else
        state.stats
        |> Map.put(component.pipeline_module, %{})
        |> put_in(
          [component.pipeline_module, :since],
          DateTime.truncate(DateTime.utc_now(), :microsecond)
        )
      end

    stage_set_stats = get_in(stats, [component.pipeline_module, component.stage_set_ref])

    stats =
      if stage_set_stats do
        counter = get_in(stats, [component.pipeline_module, component.stage_set_ref, :counter])

        sum_time_micro =
          get_in(stats, [
            component.pipeline_module,
            component.stage_set_ref,
            :sum_time_micro
          ])

        stats
        |> put_in([component.pipeline_module, component.stage_set_ref], %{
          name: component.name,
          counter: counter + 1,
          sum_time_micro: sum_time_micro + duration_micro
        })
      else
        stats
        |> put_in([component.pipeline_module, component.stage_set_ref], %{
          name: component.name,
          counter: 1,
          sum_time_micro: duration_micro
        })
      end

    {:noreply, %{state | stats: stats}}
  end

  defp do_attach_telemetry(pid) do
    :ok =
      :telemetry.attach_many(
        "telemetry-stats",
        [
          [:alf, :component, :stop]
        ],
        &Handler.handle_event/4,
        %{pid: pid}
      )
  end
end
