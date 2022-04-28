defmodule ALF.AutoScaler do
  use GenServer

  defstruct pid: nil,
            pipelines: []

  alias ALF.{Manager, PerformanceStats}

  @interval 1000

  def start_link([]) do
    GenServer.start_link(__MODULE__, %__MODULE__{}, name: __MODULE__)
  end

  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    {:ok, state, {:continue, :spawn_monitor}}
  end

  def handle_continue(:spawn_monitor, %__MODULE__{} = state) do
    spawn_scaling_attempts(state.pid)
    {:noreply, state}
  end

  defp spawn_scaling_attempts(pid) do
    Process.send_after(pid, :monitor_workload, @interval)
  end

  def handle_info(:monitor_workload, state) do
    state.pipelines
    |> Enum.each(fn pipeline ->
      stats = PerformanceStats.stats_for(pipeline)
      ips_count = Manager.producer_ips_count(pipeline)

      cond do
        ips_count > Manager.max_producer_load() * 0.9 ->
          scale_up(pipeline, stats)
          PerformanceStats.reset_stats_for(pipeline)

        ips_count < Manager.max_producer_load() * 0.1 ->
          case scale_down(pipeline, stats) do
            {:error, :only_one_left} ->
              :ok

            _removed_stage ->
              PerformanceStats.reset_stats_for(pipeline)
          end

        true ->
          :ok
      end
    end)

    spawn_scaling_attempts(state.pid)
    {:noreply, state}
  end

  defp scale_up(_pipeline, nil), do: :no_stats

  defp scale_up(pipeline, stats) do
    Manager.reload_components_states(pipeline)

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
      |> Enum.filter(fn {_stage_set_ref, stage_stats} ->
        map_size(stage_stats) > 1
      end)
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

  @spec register_pipeline(atom()) :: atom()
  def register_pipeline(module), do: GenServer.call(__MODULE__, {:register_pipeline, module})

  @spec unregister_pipeline(atom()) :: atom()
  def unregister_pipeline(module), do: GenServer.call(__MODULE__, {:unregister_pipeline, module})

  @spec pipelines() :: list(atom())
  def pipelines(), do: GenServer.call(__MODULE__, :pipelines)

  def handle_call({:register_pipeline, pipeline}, _from, state) do
    state = %{state | pipelines: [pipeline | state.pipelines]}
    {:reply, pipeline, state}
  end

  def handle_call({:unregister_pipeline, pipeline}, _from, state) do
    pipelines = Enum.filter(state.pipelines, &(&1 != pipeline))
    state = %{state | pipelines: pipelines}
    {:reply, pipeline, state}
  end

  def handle_call(:pipelines, _from, state) do
    {:reply, state.pipelines, state}
  end
end
