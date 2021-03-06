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
      on_exit(fn -> Manager.stop(PipelineToScaleUp) end)
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
      on_exit(fn -> Manager.stop(PipelineToScaleDown) end)
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

    test "down and then up, check stages_to_be_deleted" do
      init_components_pids =
        PipelineToScaleDown
        |> Manager.components()
        |> Enum.map(& &1.pid)

      {:ok, pid} = Client.start(PipelineToScaleDown)

      1..50
      |> Enum.each(fn event ->
        Client.call(pid, event)
        Process.sleep(10)
      end)

      components = Manager.reload_components_states(PipelineToScaleDown)
      assert length(components) == 4

      1..300
      |> Manager.stream_to(PipelineToScaleDown)
      |> Enum.to_list()

      components = Manager.reload_components_states(PipelineToScaleDown)
      assert length(Enum.filter(components, &(&1.name == :add_one))) > 1
      assert length(Enum.filter(components, &(&1.name == :mult_two))) > 1

      components_pids =
        PipelineToScaleDown
        |> Manager.components()
        |> Enum.map(& &1.pid)

      (init_components_pids -- components_pids)
      |> Enum.each(fn pid ->
        refute Process.alive?(pid)
      end)
    end
  end

  describe "scaling down when there is one very fast component" do
    defmodule PipelineToScaleDown2 do
      use ALF.DSL

      @components [
        stage(:do_nothing),
        stage(:add_one, count: 2),
        stage(:mult_two, count: 2)
      ]

      def do_nothing(event, _), do: event

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
      Manager.start(PipelineToScaleDown2, autoscaling_enabled: true, telemetry_enabled: true)
    end

    test "down" do
      {:ok, pid} = Client.start(PipelineToScaleDown2)

      1..50
      |> Enum.each(fn event ->
        Client.call(pid, event)
        Process.sleep(10)
      end)

      components = Manager.reload_components_states(PipelineToScaleDown2)
      assert length(components) == 5
    end
  end
end
