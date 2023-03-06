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

    test "with opts" do
      Manager.start(ExtremelySimplePipeline, telemetry_enabled: true)
      state = Manager.__state__(ExtremelySimplePipeline)

      %Manager{
        name: ExtremelySimplePipeline,
        pipeline_module: ExtremelySimplePipeline,
        pipeline: %ALF.Pipeline{},
        telemetry_enabled: true
      } = state
    end

    test "with invalid opts" do
      assert_raise RuntimeError,
                   "Wrong options for the 'simple_pipeline' pipeline: [:a]. " <>
                     "Available options are [:telemetry_enabled, :sync]",
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
        sup_pid: sup_pid
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
      Manager.start(SimplePipelineToStop)
      state = Manager.__state__(SimplePipelineToStop)

      [stage] = state.pipeline.components
      producer = state.pipeline.producer
      consumer = state.pipeline.consumer
      on_exit(fn -> Manager.stop(SimplePipelineToStop) end)
      %{state: state, stage: stage, producer: producer, consumer: consumer}
    end

    test "stop pipeline", %{stage: stage, producer: producer, consumer: consumer} do
      state = Manager.stop(SimplePipelineToStop)

      refute Process.alive?(stage.pid)
      refute Process.alive?(producer.pid)
      refute Process.alive?(consumer.pid)
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

  describe "call/2" do
    defmodule SimplePipelineToCall do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _) do
        event + 1
      end

      def mult_two(event, _), do: event * 2
    end

    setup do
      SimplePipelineToCall.start()
      on_exit(&SimplePipelineToCall.stop/0)
    end

    test "run stream and check events" do
      assert SimplePipelineToCall.call(1) == 4
    end

    test "SimplePipelineToCall.call" do
      assert SimplePipelineToCall.call(1) == 4
    end

    test "with return ip option" do
      assert %ALF.IP{event: 4} = SimplePipelineToCall.call(1, return_ip: true)
    end

    test "call from many Tasks" do
      1..10
      |> Enum.map(fn _event ->
        Task.async(fn ->
          assert SimplePipelineToCall.call(1) == 4
        end)
      end)
      |> Task.await_many()
    end
  end

  describe "sync call" do
    defmodule SimplePipelineToSyncCall do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _) do
        event + 1
      end

      def mult_two(event, _), do: event * 2
    end

    setup do
      SimplePipelineToSyncCall.start(sync: true)
      on_exit(&SimplePipelineToSyncCall.stop/0)
    end

    test "run stream and check events" do
      assert SimplePipelineToSyncCall.call(1) == 4
      assert %ALF.IP{event: 4} = SimplePipelineToSyncCall.call(1, return_ip: true)
    end
  end

  describe "stream/3" do
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
        |> Manager.stream(SimplePipelineToStream)
        |> Enum.to_list()

      assert results == [4, 6, 8]
    end

    test "run several streams at once" do
      stream1 = Manager.stream(1..100, SimplePipelineToStream)
      stream2 = Manager.stream(101..200, SimplePipelineToStream)
      stream3 = Manager.stream(201..300, SimplePipelineToStream)

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
        |> Manager.stream(SimplePipelineToStream, return_ips: true)
        |> Enum.to_list()

      assert [
               %ALF.IP{event: 4},
               %ALF.IP{event: 6},
               %ALF.IP{event: 8}
             ] = results
    end
  end

  describe "sync stream/2" do
    defmodule SimplePipelineToSyncStream do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _) do
        event + 1
      end

      def mult_two(event, _), do: event * 2
    end

    @sample_stream [1, 2, 3]

    setup do
      SimplePipelineToSyncStream.start(sync: true)
    end

    test "stream" do
      results =
        @sample_stream
        |> SimplePipelineToSyncStream.stream()
        |> Enum.to_list()

      assert results == [4, 6, 8]
    end

    test "stream with return_ips option" do
      assert [%ALF.IP{event: 4}, %ALF.IP{event: 6}, %ALF.IP{event: 8}] =
               @sample_stream
               |> SimplePipelineToSyncStream.stream(return_ips: true)
               |> Enum.to_list()
    end
  end

  describe "stream with decomposer" do
    defmodule DecomposerPipeline do
      use ALF.DSL

      @components [
        decomposer(:decomposer_function)
      ]

      def decomposer_function(event, _) do
        String.split(event)
      end
    end

    setup do
      DecomposerPipeline.start()
      on_exit(&DecomposerPipeline.stop/0)
    end

    test "call" do
      assert DecomposerPipeline.call("aaa") == "aaa"
      assert DecomposerPipeline.call("aaa bbb ccc") == ["bbb", "aaa", "ccc"]
    end

    test "stream" do
      results =
        ["aaa bbb ccc", "ddd eee", "xxx"]
        |> DecomposerPipeline.stream()
        |> Enum.to_list()

      assert length(results) == 6
    end
  end

  describe "stream with recomposer" do
    defmodule RecomposerPipeline do
      use ALF.DSL

      @components [
        recomposer(:recomposer_function)
      ]

      def recomposer_function(event, prev_events, _) do
        string = Enum.join(prev_events ++ [event], " ")

        if String.length(string) >= 5 do
          string
        else
          :continue
        end
      end
    end

    setup do
      RecomposerPipeline.start()
      on_exit(&RecomposerPipeline.stop/0)
    end

    test "call" do
      assert RecomposerPipeline.call("aaaaa") == "aaaaa"
      assert is_nil(RecomposerPipeline.call("aaa"))
      assert RecomposerPipeline.call("bbb") == "aaa bbb"
    end

    test "stream" do
      ["aa", "bb", "xxxxx"]
      |> RecomposerPipeline.stream()
      |> Enum.to_list()
    end
  end

  describe "timeout with call" do
    defmodule TimeoutPipeline do
      use ALF.DSL

      @components [
        stage(:sleep)
      ]

      def sleep(event, _) do
        Process.sleep(10)
        event + 1
      end
    end

    setup do
      TimeoutPipeline.start()
      on_exit(&TimeoutPipeline.stop/0)
    end

    test "run stream and check events" do
      assert %ALF.ErrorIP{error: :timeout} = TimeoutPipeline.call(1, timeout: 5)
    end
  end

  describe "timeout with stream" do
    defmodule TimeoutPipelineStream do
      use ALF.DSL

      @components [
        stage(:sleep)
      ]

      def sleep(event, _) do
        if event == 2 do
          Process.sleep(10)
        end

        event + 1
      end
    end

    setup do
      TimeoutPipelineStream.start()
      on_exit(&TimeoutPipelineStream.stop/0)
    end

    test "run stream and check events" do
      results =
        0..3
        |> TimeoutPipelineStream.stream(timeout: 5)
        |> Enum.to_list()

      errors =
        results
        |> Enum.filter(&is_struct(&1, ALF.ErrorIP))
        |> Enum.map(& &1.error)
        |> Enum.uniq()

      assert errors == [:timeout]
    end
  end

  describe "cast" do
    defmodule PipelineToCast do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _), do: event + 1
      def mult_two(event, _), do: event * 2
    end

    setup do
      PipelineToCast.start()
      on_exit(&PipelineToCast.stop/0)
    end

    test "cast with send_result true" do
      ref = PipelineToCast.cast(1, send_result: true)
      assert is_reference(ref)

      receive do
        {^ref, %ALF.IP{event: event}} ->
          assert event == 4
      end
    end

    test "cast with send_result false" do
      ref = PipelineToCast.cast(1, send_result: false)
      assert is_reference(ref)

      receive do
        _any ->
          assert false
      after
        10 ->
          assert true
      end
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
