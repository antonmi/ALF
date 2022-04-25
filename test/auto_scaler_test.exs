defmodule ALF.AutoScalerTest do
  use ExUnit.Case, async: false

  alias ALF.{Manager, AutoScaler}

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

      stats = AutoScaler.stats_for(SimplePipeline)

      stats
      |> Enum.each(fn
        {:since, date_time} ->
          assert %DateTime{} = date_time

        {ref, %{{:add_one, 0} => data}} when is_reference(ref) ->
          assert data[:counter] == 3
          assert data[:sum_time_micro] > 0

        {ref, %{{:mult_two, 0} => data}} when is_reference(ref) ->
          assert data[:counter] == 3
          assert data[:sum_time_micro] > 0
      end)

      AutoScaler.reset_stats_for(SimplePipeline)
      stats = AutoScaler.stats_for(SimplePipeline)
      assert is_nil(stats)
    end
  end

  describe "register_pipeline/1 and pipelines/0" do
    test "registering" do
      AutoScaler.register_pipeline(SimplePipeline)
      assert Enum.member?(AutoScaler.pipelines(), SimplePipeline)
    end
  end

  describe "scaling" do
    defmodule PipelineToScale do
      use ALF.DSL

      @components [
        stage(:add_one, count: 2),
        stage(:mult_two, count: 2)
      ]

      def add_one(event, _) do
        Process.sleep(10)
        event + 1
      end

      def mult_two(event, _) do
        Process.sleep(20)
        event * 2
      end
    end

    setup do
      before = Application.get_env(:alf, :telemetry_enabled)
      Application.put_env(:alf, :telemetry_enabled, true)
      on_exit(fn -> Application.put_env(:alf, :telemetry_enabled, before) end)
      Manager.start(PipelineToScale)
    end

    test "test" do
      1..5
      |> Manager.stream_to(PipelineToScale)
      |> Enum.to_list()
      Process.sleep(1000)

#      IO.inspect("111111111111111111111111111111111111111111111111111111")
#      IO.inspect("111111111111111111111111111111111111111111111111111111")
#      IO.inspect("111111111111111111111111111111111111111111111111111111")
#      IO.inspect("111111111111111111111111111111111111111111111111111111")
#      IO.inspect("111111111111111111111111111111111111111111111111111111")
#      Process.sleep(1000)
#
#      1..5
#      |> Manager.stream_to(PipelineToScale)
#      |> Enum.to_list()
#      Process.sleep(1000)
#
#      1..5
#      |> Manager.stream_to(PipelineToScale)
#      |> Enum.to_list()
#      Process.sleep(1000)
#
#      1..5
#      |> Manager.stream_to(PipelineToScale)
#      |> Enum.to_list()
    end
  end
end
