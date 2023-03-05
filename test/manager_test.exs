defmodule ALF.ManagerTest do
  use ExUnit.Case, async: false

  alias ALF.Manager

  describe "start" do
    defmodule ExtremelySimplePipeline do
      use ALF.DSL

      @components []
    end

    setup do
      on_exit(fn -> Manager.stop(ExtremelySimplePipeline) end)
    end

    test "with just module name" do
      Manager.start(ExtremelySimplePipeline)
      state = Manager.__state__(ExtremelySimplePipeline)

      %Manager{
        name: ExtremelySimplePipeline,
        pipeline_module: ExtremelySimplePipeline,
        pipeline: %ALF.Pipeline{},
        registry: %{},
        autoscaling_enabled: false,
        telemetry_enabled: false
      } = state
    end

    test "telemetry default" do
      Manager.start(ExtremelySimplePipeline)
      state = Manager.__state__(ExtremelySimplePipeline)

      %Manager{telemetry_enabled: false} = state
    end

    test "telemetry default when it's enabled in configs" do
      before = Application.get_env(:alf, :telemetry_enabled)
      Application.put_env(:alf, :telemetry_enabled, true)
      on_exit(fn -> Application.put_env(:alf, :telemetry_enabled, before) end)

      Manager.start(ExtremelySimplePipeline)
      state = Manager.__state__(ExtremelySimplePipeline)

      %Manager{telemetry_enabled: true} = state
    end

    test "with custom name" do
      Manager.start(ExtremelySimplePipeline, :simple_pipeline)
      state = Manager.__state__(:simple_pipeline)

      %Manager{
        name: :simple_pipeline,
        pipeline_module: ExtremelySimplePipeline,
        pipeline: %ALF.Pipeline{},
        registry: %{}
      } = state
    end

    test "with opts" do
      Manager.start(ExtremelySimplePipeline, autoscaling_enabled: true, telemetry_enabled: true)
      state = Manager.__state__(ExtremelySimplePipeline)

      %Manager{
        name: ExtremelySimplePipeline,
        pipeline_module: ExtremelySimplePipeline,
        pipeline: %ALF.Pipeline{},
        autoscaling_enabled: true,
        telemetry_enabled: true,
        registry: %{}
      } = state
    end

    test "with invalid opts" do
      assert_raise RuntimeError,
                   "Wrong options for the 'simple_pipeline' pipeline: [:a]. " <>
                     "Available options are [:autoscaling_enabled, :telemetry_enabled, :sync]",
                   fn ->
                     Manager.start(ExtremelySimplePipeline, :simple_pipeline, a: :b)
                   end
    end
  end

  describe "start-up actions" do
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
      Manager.start(SimplePipeline)
      state = Manager.__state__(SimplePipeline)
      on_exit(fn -> Manager.stop(SimplePipeline) end)
      %{state: state}
    end

    test "state after start", %{state: state} do
      %Manager{
        name: SimplePipeline,
        pipeline_module: SimplePipeline,
        pid: pid,
        pipeline: %ALF.Pipeline{},
        components: _components,
        pipeline_sup_pid: pipeline_sup_pid,
        sup_pid: sup_pid,
        registry: %{}
      } = state

      assert is_pid(pid)
      assert is_pid(pipeline_sup_pid)
      assert is_pid(sup_pid)
    end

    test "components", %{state: state} do
      %Manager{components: [producer, add, mult, consumer]} = state

      assert producer.name == :producer
      assert is_pid(producer.pid)

      assert add.name == :add_one
      assert add.subscribe_to == [{producer.pid, [max_demand: 1, cancel: :transient]}]
      producer_pid = producer.pid
      assert [{^producer_pid, _ref}] = add.subscribed_to
      mult_pid = mult.pid
      assert [{^mult_pid, _ref}] = add.subscribers

      assert mult.name == :mult_two
      assert mult.subscribe_to == [{add.pid, [max_demand: 1, cancel: :transient]}]
      add_pid = add.pid
      assert [{^add_pid, _ref}] = mult.subscribed_to
      consumer_pid = consumer.pid
      assert [{^consumer_pid, _ref}] = mult.subscribers

      assert consumer.name == :consumer
      assert consumer.subscribe_to == [{mult.pid, [max_demand: 1, cancel: :transient]}]
      mult_pid = mult.pid
      assert [{^mult_pid, _ref}] = consumer.subscribed_to
    end
  end

  test "start non-existing pipeline" do
    assert_raise RuntimeError,
                 "The Elixir.NoSuchPipeline doesn't implement any pipeline",
                 fn -> Manager.start(NoSuchPipeline) end
  end

  describe "stop/1" do
    defmodule SimplePipelineToStop do
      use ALF.DSL

      @components [
        stage(:add_one)
      ]

      def add_one(event, _), do: event + 1
    end

    setup do
      Manager.start(SimplePipelineToStop, :pipeline_to_stop)
      state = Manager.__state__(:pipeline_to_stop)
      on_exit(fn -> Manager.stop(SimplePipelineToStop) end)
      %{state: state}
    end

    test "stop pipeline" do
      state = Manager.stop(:pipeline_to_stop)

      refute Process.alive?(state.pid)
      refute Process.alive?(state.pipeline_sup_pid)

      state.components
      |> Enum.each(fn component ->
        refute Process.alive?(component.pid)
      end)
    end
  end

  describe "stop/1 for sync pipeline" do
    defmodule SimplePipelineToStop2 do
      use ALF.DSL

      @components [
        stage(:add_one)
      ]

      def add_one(event, _), do: event + 1
    end

    setup do
      Manager.start(SimplePipelineToStop2, sync: true)
      state = Manager.__state__(SimplePipelineToStop2)
      on_exit(fn -> Manager.stop(SimplePipelineToStop2) end)
      %{state: state}
    end

    test "stop sync pipeline" do
      state = Manager.stop(SimplePipelineToStop2)

      refute Process.alive?(state.pid)
    end
  end

  describe "prepare gotos after initialization" do
    defmodule GoToPipeline do
      use ALF.DSL

      @components [
        goto_point(:point),
        goto(:goto, to: :point)
      ]
    end

    setup do
      Manager.start(GoToPipeline)
      Process.sleep(5)

      state = Manager.__state__(GoToPipeline)
      on_exit(fn -> Manager.stop(GoToPipeline) end)
      %{state: state}
    end

    test "set to_pid in goto component", %{state: state} do
      %Manager{components: [_producer, point, goto, _consumer]} = state
      assert is_pid(point.pid)
      assert goto.to_pid == point.pid
    end
  end

  describe "stream_to/2" do
    defmodule SimplePipelineToStream do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _), do: event + 1
      def mult_two(event, _), do: event * 2
    end

    def sample_stream, do: [1, 2, 3]

    setup do
      Manager.start(SimplePipelineToStream)
      on_exit(fn -> Manager.stop(SimplePipelineToStream) end)
    end

    test "run stream and check events" do
      results =
        sample_stream()
        |> Manager.stream_to(SimplePipelineToStream)
        |> Enum.to_list()

      assert results == [4, 6, 8]
    end

    test "run several streams at once" do
      stream1 = Manager.stream_to(1..100, SimplePipelineToStream)
      stream2 = Manager.stream_to(101..200, SimplePipelineToStream)
      stream3 = Manager.stream_to(201..300, SimplePipelineToStream)

      [result1, result2, result3] =
        [stream1, stream2, stream3]
        |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
        |> Task.await_many()

      assert Enum.sort(result1) == Enum.map(1..100, &((&1 + 1) * 2))
      assert Enum.sort(result2) == Enum.map(101..200, &((&1 + 1) * 2))
      assert Enum.sort(result3) == Enum.map(201..300, &((&1 + 1) * 2))
    end

    test "run with return_ips: true option" do
      results =
        sample_stream()
        |> Manager.stream_to(SimplePipelineToStream, return_ips: true)
        |> Enum.to_list()

      assert [
               %ALF.IP{event: 4},
               %ALF.IP{event: 6},
               %ALF.IP{event: 8}
             ] = results
    end
  end

  describe "components/1" do
    defmodule SimplePipelineWithComponents do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _), do: event + 1
      def mult_two(event, _), do: event * 2
    end

    setup do
      Manager.start(SimplePipelineWithComponents)
      on_exit(fn -> Manager.stop(SimplePipelineWithComponents) end)
    end

    test "get components module" do
      components = Manager.components(SimplePipelineWithComponents)
      assert length(components) == 4
    end

    test "reloading" do
      components = Manager.reload_components_states(SimplePipelineWithComponents)
      assert length(components) == 4
    end
  end
end
