defmodule ALF.AutoScaler do
  use GenServer

  defstruct pid: nil,
            pipelines: [],
            stats: %{}

  alias ALF.Manager

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
    {:noreply, state, {:continue, :spawn_monitor}}
  end

  def handle_continue(:spawn_monitor, %__MODULE__{} = state) do
    spawn_scaling_attempts(state.pid)
    {:noreply, state}
  end

  defp spawn_scaling_attempts(pid) do
    Process.send_after(pid, :monitor_workload, 1000)
  end

  def handle_info(:monitor_workload, state) do
    new_stats =
      state.pipelines
      |> Enum.reduce(state.stats, fn pipeline, acc ->
        ips_count = Manager.producer_ips_count(pipeline)

        cond do
          ips_count > Manager.max_producer_load() * 0.9 ->
            scale_up(pipeline, Map.get(state.stats, pipeline))
            Map.put(acc, pipeline, nil)

          ips_count < Manager.max_producer_load() * 0.1 ->
            case scale_down(pipeline, Map.get(state.stats, pipeline)) do
              {:error, :only_one_left} ->
                acc

              _removed_stage ->
                Map.put(acc, pipeline, nil)
            end

          true ->
            acc
        end
      end)

    spawn_scaling_attempts(state.pid)
    {:noreply, %{state | stats: new_stats}}
  end

  defp scale_up(_pipeline, nil), do: :no_stats

  defp scale_up(pipeline, stats) do
    {slowest_stage_set_ref, _} =
      stats
      |> Enum.min_by(fn
        {:since, _time} ->
          :atom_is_more_than_number

        {_stage_set_ref, stage_stats} ->
          total_stage_set_speed(stage_stats)
      end)

    Manager.add_component(pipeline, slowest_stage_set_ref)
  end

  defp scale_down(_pipeline, nil), do: :no_stats

  defp scale_down(pipeline, stats) do
    Manager.reload_components_states(pipeline)

    {fastest_stage_set_ref, _} =
      stats
      |> Enum.max_by(fn
        {:since, _time} ->
          -1

        {_stage_set_ref, stage_stats} ->
          total_stage_set_speed(stage_stats)
      end)

    Manager.remove_component(pipeline, fastest_stage_set_ref)
  end

  defp total_stage_set_speed(stage_stats) do
    Enum.reduce(stage_stats, 0, fn {_key, data}, speed ->
      speed + data[:counter] / data[:sum_time_micro]
    end)
  end

  @spec stats_for(atom()) :: map()
  def stats_for(pipeline), do: GenServer.call(__MODULE__, {:stats_for, pipeline})

  @spec reset_stats_for(atom()) :: :ok
  def reset_stats_for(pipeline), do: GenServer.call(__MODULE__, {:reset_stats_for, pipeline})

  @spec register_pipeline(atom()) :: atom()
  def register_pipeline(module), do: GenServer.call(__MODULE__, {:register_pipeline, module})

  @spec pipelines() :: list(atom())
  def pipelines(), do: GenServer.call(__MODULE__, :pipelines)

  def handle_call({:stats_for, pipeline}, _from, state) do
    {:reply, Map.get(state.stats, pipeline, nil), state}
  end

  def handle_call({:reset_stats_for, pipeline}, _from, state) do
    stats = Map.delete(state.stats, pipeline)
    {:reply, :ok, %{state | stats: stats}}
  end

  def handle_call({:register_pipeline, pipeline}, _from, state) do
    state = %{state | pipelines: [pipeline | state.pipelines]}
    {:reply, pipeline, state}
  end

  def handle_call(:pipelines, _from, state) do
    {:reply, state.pipelines, state}
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
        "telemetry-stats",
        [
          [:alf, :component, :stop]
        ],
        &Handler.handle_event/4,
        %{pid: pid}
      )
  end
end
