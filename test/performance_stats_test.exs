defmodule ALF.PerformanceStatsTest do
  use ExUnit.Case, async: false

  alias ALF.{PerformanceStats, Manager}

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(:add_one),
      stage(:mult_two)
    ]

    def add_one(event, _), do: event + 1
    def mult_two(event, _), do: event * 2
  end

  describe "stats" do
    setup do
      Manager.start(SimplePipeline, autoscaling_enabled: true, telemetry_enabled: true)
    end

    test "stats for the pipeline" do
      [1, 2, 3]
      |> Manager.stream_to(SimplePipeline)
      |> Enum.to_list()

      Process.sleep(5)
      stats = PerformanceStats.stats_for(SimplePipeline)

      stats
      |> Enum.each(fn
        {:since, date_time} ->
          assert %DateTime{} = date_time

        {:producer, data} ->
          assert data[:counter] == 3

        {:consumer, data} ->
          assert data[:counter] == 3

        {ref, %{{:add_one, 0} => data}} when is_reference(ref) ->
          assert data[:counter] == 3
          assert data[:sum_time_micro] > 0

        {ref, %{{:mult_two, 0} => data}} when is_reference(ref) ->
          assert data[:counter] == 3
          assert data[:sum_time_micro] > 0
      end)

      PerformanceStats.reset_stats_for(SimplePipeline)
      stats = PerformanceStats.stats_for(SimplePipeline)
      assert is_nil(stats)
    end
  end
end
