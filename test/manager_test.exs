defmodule ALF.ManagerTest do
  use ExUnit.Case, async: false

  alias ALF.{IP, Manager}

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
                     "Available options are [:autoscaling_enabled, :telemetry_enabled]",
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

    test "run with options" do
      results =
        sample_stream()
        |> Manager.stream_to(SimplePipelineToStream, chunk_every: 5)
        |> Enum.to_list()

      assert results == [4, 6, 8]
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

  describe "steam_with_ids_to/2" do
    defmodule SimplePipelineToStreamWitIds do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _), do: event + 1
      def mult_two(event, _), do: event * 2
    end

    def sample_stream_with_ids(ref, pid) do
      [{ref, 1}, {:my_id, 2}, {pid, 3}]
    end

    setup do
      Manager.start(SimplePipelineToStreamWitIds)
      on_exit(fn -> Manager.stop(SimplePipelineToStreamWitIds) end)
    end

    test "run stream and check events" do
      ref = make_ref()
      pid = self()

      result =
        sample_stream_with_ids(ref, pid)
        |> Manager.steam_with_ids_to(SimplePipelineToStreamWitIds)
        |> Enum.to_list()

      assert [{^ref, 4}, {:my_id, 6}, {^pid, 8}] = result
    end

    test "run stream with return_ips: true option" do
      ref = make_ref()
      pid = self()

      result =
        sample_stream_with_ids(ref, pid)
        |> Manager.steam_with_ids_to(SimplePipelineToStreamWitIds, return_ips: true)
        |> Enum.to_list()

      assert [
               {^ref, %IP{ref: ^ref, event: 4}},
               {:my_id, %IP{ref: :my_id, event: 6}},
               {^pid, %IP{ref: ^pid, event: 8}}
             ] = result
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

  describe "add_component" do
    defmodule PipelineToScale do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _) do
        Process.sleep(1)
        event + 1
      end

      def mult_two(event, _) do
        Process.sleep(1)
        event * 2
      end
    end

    setup do
      Manager.start(PipelineToScale)
      on_exit(fn -> Manager.stop(PipelineToScale) end)
      %{components: Manager.components(PipelineToScale)}
    end

    test "add_component to add_one and then to mult_two", %{components: init_components} do
      component = Enum.find(init_components, &(&1.name == :add_one))
      Manager.add_component(PipelineToScale, component.stage_set_ref)

      Process.sleep(1)
      components = Manager.reload_components_states(PipelineToScale)

      producer = Enum.find(components, &(&1.name == :producer))
      [add_one1, add_one2] = Enum.filter(components, &(&1.name == :add_one))

      subscriber_pids = Enum.map(producer.subscribers, fn {pid, _ref} -> pid end)
      assert Enum.count(producer.subscribers) == 2
      assert Enum.member?(subscriber_pids, add_one1.pid)
      assert Enum.member?(subscriber_pids, add_one2.pid)

      producer_pid = producer.pid
      assert [{^producer_pid, _opts}] = add_one1.subscribed_to
      assert [{^producer_pid, _opts}] = add_one2.subscribed_to

      mult_two_component = Enum.find(components, &(&1.name == :mult_two))

      [{pid1, _ref1}, {pid2, _ref2}] = mult_two_component.subscribed_to
      assert Enum.member?([pid1, pid2], add_one1.pid)
      assert Enum.member?([pid1, pid2], add_one2.pid)

      assert add_one1.count == 2
      assert add_one2.count == 2
      assert add_one1.stage_set_ref == add_one2.stage_set_ref
      assert abs(add_one1.number - add_one2.number) == 1

      # Add to mult_two
      Manager.add_component(PipelineToScale, mult_two_component.stage_set_ref)
      Process.sleep(1)
      components = Manager.reload_components_states(PipelineToScale)

      [add_one1, add_one2] = Enum.filter(components, &(&1.name == :add_one))
      [mult_two1, mult_two2] = Enum.filter(components, &(&1.name == :mult_two))
      consumer = Enum.find(components, &(&1.name == :consumer))

      subscriber_pids = Enum.map(add_one1.subscribers, fn {pid, _ref} -> pid end)
      assert Enum.count(subscriber_pids) == 2
      assert Enum.member?(subscriber_pids, mult_two1.pid)
      assert Enum.member?(subscriber_pids, mult_two1.pid)

      subscriber_pids = Enum.map(add_one2.subscribers, fn {pid, _ref} -> pid end)
      assert Enum.count(subscriber_pids) == 2
      assert Enum.member?(subscriber_pids, mult_two1.pid)
      assert Enum.member?(subscriber_pids, mult_two1.pid)

      assert add_one1.count == 2
      assert add_one2.count == 2
      assert abs(add_one1.number - add_one2.number) == 1

      [{pid1, _ref1}, {pid2, _ref2}] = mult_two1.subscribed_to
      assert Enum.member?([pid1, pid2], add_one1.pid)
      assert Enum.member?([pid1, pid2], add_one2.pid)

      [{pid1, _ref1}, {pid2, _ref12}] = mult_two2.subscribed_to
      assert Enum.member?([pid1, pid2], add_one1.pid)
      assert Enum.member?([pid1, pid2], add_one2.pid)

      assert mult_two1.count == 2
      assert mult_two2.count == 2
      assert abs(mult_two1.number - mult_two2.number) == 1

      consumer_pid = consumer.pid
      assert [{^consumer_pid, _ref}] = mult_two1.subscribers
      assert [{^consumer_pid, _ref}] = mult_two2.subscribers

      [{pid1, _ref1}, {pid2, _ref2}] = consumer.subscribed_to
      assert Enum.member?([pid1, pid2], mult_two1.pid)
      assert Enum.member?([pid1, pid2], mult_two2.pid)
    end
  end

  describe "remove_component simple case" do
    defmodule PipelineToRemove do
      use ALF.DSL

      @components [
        stage(:add_one, count: 2)
      ]

      def add_one(event, _) do
        event + 1
      end
    end

    setup do
      Manager.start(PipelineToRemove)

      on_exit(fn -> Manager.stop(PipelineToRemove) end)
      %{components: Manager.components(PipelineToRemove)}
    end

    test "remove add_one", %{components: init_components} do
      component = Enum.find(init_components, &(&1.name == :add_one))
      _removed_stage = Manager.remove_component(PipelineToRemove, component.stage_set_ref)

      Process.sleep(1)
      [producer, add_one, consumer] = Manager.reload_components_states(PipelineToRemove)

      add_one_pid = add_one.pid
      assert [{^add_one_pid, _ref}] = producer.subscribers

      producer_pid = producer.pid
      consumer_pid = consumer.pid

      assert [{^producer_pid, _ref}] = add_one.subscribed_to
      assert [{^consumer_pid, _ref}] = add_one.subscribers

      assert [{^add_one_pid, _ref}] = consumer.subscribed_to
    end
  end

  describe "remove_component after adding" do
    defmodule PipelineToScale2 do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _) do
        Process.sleep(1)
        event + 1
      end

      def mult_two(event, _) do
        Process.sleep(1)
        event * 2
      end
    end

    setup do
      Manager.start(PipelineToScale2)
      init_components = Manager.components(PipelineToScale2)
      component = Enum.find(init_components, &(&1.name == :add_one))
      Manager.add_component(PipelineToScale2, component.stage_set_ref)

      component = Enum.find(Manager.components(PipelineToScale2), &(&1.name == :mult_two))
      Manager.add_component(PipelineToScale2, component.stage_set_ref)

      on_exit(fn -> Manager.stop(PipelineToScale2) end)
      Process.sleep(5)
      %{components: Manager.reload_components_states(PipelineToScale2)}
    end

    test "remove add_one and then to mult_two", %{components: init_components} do
      component = Enum.find(init_components, &(&1.name == :add_one))
      _removed_stage = Manager.remove_component(PipelineToScale2, component.stage_set_ref)

      Process.sleep(1)
      components = Manager.reload_components_states(PipelineToScale2)

      producer = Enum.find(components, &(&1.name == :producer))
      [add_one] = Enum.filter(components, &(&1.name == :add_one))
      [mult_two1, mult_two2] = Enum.filter(components, &(&1.name == :mult_two))

      add_one_pid = add_one.pid
      assert [{^add_one_pid, _ref}] = producer.subscribers
      producer_pid = producer.pid
      assert [{^producer_pid, _ref}] = add_one.subscribed_to
      assert add_one.count == 1
      assert add_one.number == 0

      assert [{^producer_pid, _ref}] = add_one.subscribed_to

      subscriber_pids = Enum.map(add_one.subscribers, fn {pid, _ref} -> pid end)
      assert Enum.member?(subscriber_pids, mult_two1.pid)
      assert Enum.member?(subscriber_pids, mult_two2.pid)

      [{^add_one_pid, _ref1}] = mult_two1.subscribed_to
      [{^add_one_pid, _ref1}] = mult_two2.subscribed_to

      # remove mult_two
      component = Enum.find(components, &(&1.name == :mult_two))
      _removed_stage = Manager.remove_component(PipelineToScale2, component.stage_set_ref)

      Process.sleep(1)
      components = Manager.reload_components_states(PipelineToScale2)

      [add_one] = Enum.filter(components, &(&1.name == :add_one))
      [mult_two] = Enum.filter(components, &(&1.name == :mult_two))
      consumer = Enum.find(components, &(&1.name == :consumer))

      assert add_one.count == 1
      assert add_one.number == 0

      assert mult_two.count == 1
      assert mult_two.number == 0

      mult_two_pid = mult_two.pid
      assert [{^mult_two_pid, _ref}] = add_one.subscribers
      add_one_pid = add_one.pid

      assert [{^add_one_pid, _ref}] = mult_two.subscribed_to
      consumer_pid = consumer.pid
      assert [{^consumer_pid, _ref}] = mult_two.subscribers

      # try to remove one more
      assert {:error, :only_one_left} =
               Manager.remove_component(PipelineToScale2, component.stage_set_ref)
    end

    test "there is no lost ips after stopping", %{components: init_components} do
      stream1 = Manager.stream_to(0..99, PipelineToScale2)
      stream2 = Manager.stream_to(100..199, PipelineToScale2)
      stream3 = Manager.stream_to(200..299, PipelineToScale2)

      [task1, task2, task3] =
        [stream1, stream2, stream3]
        |> Enum.map(fn stream ->
          Task.async(fn -> Enum.to_list(stream) end)
        end)

      Process.sleep(100)
      component = Enum.find(init_components, &(&1.name == :add_one))
      Manager.remove_component(PipelineToScale2, component.stage_set_ref)
      components = Manager.reload_components_states(PipelineToScale2)

      Process.sleep(100)

      component = Enum.find(components, &(&1.name == :mult_two))
      Manager.remove_component(PipelineToScale2, component.stage_set_ref)
      Manager.reload_components_states(PipelineToScale2)

      assert Task.await(task1) -- Enum.map(0..99, fn n -> (n + 1) * 2 end) == []
      assert Task.await(task2) -- Enum.map(100..199, fn n -> (n + 1) * 2 end) == []
      assert Task.await(task3) -- Enum.map(200..299, fn n -> (n + 1) * 2 end) == []
    end
  end
end
