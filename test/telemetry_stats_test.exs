defmodule ALF.TelemetryStatsTest do
  use ExUnit.Case, async: false

  alias ALF.{Manager, TelemetryStats}

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(:add_one),
      stage(:mult_two)
    ]

    def add_one(event, _), do: event + 1
    def mult_two(event, _), do: event * 2
  end

  setup do
    before = Application.get_env(:alf, :telemetry_enabled)
    Application.put_env(:alf, :telemetry_enabled, true)
    on_exit(fn -> Application.put_env(:alf, :telemetry_enabled, before) end)
    {:ok, agent} = Agent.start_link(fn -> [] end)
    %{agent: agent}
  end

  describe "stats" do
    setup do
      Manager.start(SimplePipeline)
    end

    test "stats for the pipeline" do
      [1, 2, 3]
      |> Manager.stream_to(SimplePipeline)
      |> Enum.to_list()

      stats = TelemetryStats.stats_for(SimplePipeline)
      assert stats[:since]

      stats
      |> Enum.each(fn
        {:since, date_time} ->
          assert %DateTime{} = date_time

        {ref, data} when is_reference(ref) ->
          assert data[:counter] == 3
          assert data[:name]
          assert data[:sum_time_micro] > 0
      end)
    end
  end
end
