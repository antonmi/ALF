defmodule ALF.AutoScalerTest do
  use ExUnit.Case, async: false

  alias ALF.{AutoScaler, Manager, Manager.Client}

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

      AutoScaler.unregister_pipeline(SimplePipeline)
      refute Enum.member?(AutoScaler.pipelines(), SimplePipeline)
    end
  end

  describe "scaling up" do
    defmodule PipelineToScaleUp do
      use ALF.DSL

      @components [
        stage(:add_one, count: 1),
        stage(:mult_two, count: 1)
      ]

      def add_one(event, _) do
        Process.sleep(10)
        event + 1
      end

      def mult_two(event, _) do
        Process.sleep(15)
        event * 2
      end
    end

    setup do
      Manager.start(PipelineToScaleUp, autoscaling_enabled: true, telemetry_enabled: true)
    end

    test "up" do
      1..300
      |> Manager.stream_to(PipelineToScaleUp)
      |> Enum.to_list()

      components = Manager.reload_components_states(PipelineToScaleUp)
      assert length(Enum.filter(components, &(&1.name == :add_one))) > 1
      assert length(Enum.filter(components, &(&1.name == :mult_two))) > 1
    end
  end

  describe "scaling down" do
    defmodule PipelineToScaleDown do
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
        Process.sleep(15)
        event * 2
      end
    end

    setup do
      Manager.start(PipelineToScaleDown, autoscaling_enabled: true, telemetry_enabled: true)
    end

    test "down" do
      {:ok, pid} = Client.start(PipelineToScaleDown)

      1..100
      |> Enum.each(fn event ->
        Client.call(pid, event)
        Process.sleep(10)
      end)

      components = Manager.reload_components_states(PipelineToScaleDown)
      assert length(components) == 4
    end
  end
end
