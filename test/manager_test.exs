defmodule ALF.ManagerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  alias ALF.{IP, Manager}

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(:add_one),
      stage(:mult_two)
    ]

    def add_one(event, _), do: event + 1
    def mult_two(event, _), do: event * 2
  end

  defmodule GoToPipeline do
    use ALF.DSL

    @components [
      goto_point(:point),
      goto(:goto, to: :point)
    ]
  end

  setup do
    on_exit(fn -> Manager.stop(SimplePipeline) end)
  end

  describe "start-up actions" do
    setup do
      Manager.start(SimplePipeline)
      state = Manager.__state__(SimplePipeline)
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
      mult_pid = mult.pid
      assert [{^mult_pid, _ref}] = add.subscribers

      assert mult.name == :mult_two
      assert mult.subscribe_to == [{add.pid, [max_demand: 1, cancel: :transient]}]
      consumer_pid = consumer.pid
      assert [{^consumer_pid, _ref}] = mult.subscribers

      assert consumer.name == :consumer
      assert consumer.subscribe_to == [{mult.pid, [max_demand: 1, cancel: :transient]}]
    end
  end

  test "start non-existing pipeline" do
    assert_raise RuntimeError,
                 "The Elixir.NoSuchPipeline doesn't implement any pipeline",
                 fn -> Manager.start(NoSuchPipeline) end
  end

  describe "stop/1" do
    setup do
      Manager.start(SimplePipeline, :pipeline_to_stop)
      state = Manager.__state__(:pipeline_to_stop)
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
    setup do
      Manager.start(GoToPipeline)
      Process.sleep(5)

      state = Manager.__state__(GoToPipeline)
      %{state: state}
    end

    test "set to_pid in goto component", %{state: state} do
      %Manager{components: [_producer, point, goto, _consumer]} = state
      assert is_pid(point.pid)
      assert goto.to_pid == point.pid
    end
  end

  describe "stream_to/2" do
    def sample_stream, do: [1, 2, 3]

    setup do
      Manager.start(SimplePipeline)
    end

    test "run stream and check events" do
      results =
        sample_stream()
        |> Manager.stream_to(SimplePipeline)
        |> Enum.to_list()

      assert results == [4, 6, 8]
    end

    test "run several streams at once" do
      stream1 = Manager.stream_to(1..100, SimplePipeline)
      stream2 = Manager.stream_to(101..200, SimplePipeline)
      stream3 = Manager.stream_to(201..300, SimplePipeline)

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
        |> Manager.stream_to(SimplePipeline, %{chunk_every: 5})
        |> Enum.to_list()

      assert results == [4, 6, 8]
    end

    test "run with return_ips: true option" do
      results =
        sample_stream()
        |> Manager.stream_to(SimplePipeline, %{return_ips: true})
        |> Enum.to_list()

      assert [
               %ALF.IP{event: 4},
               %ALF.IP{event: 6},
               %ALF.IP{event: 8}
             ] = results
    end
  end

  describe "steam_with_ids_to/2" do
    def sample_stream_with_ids(ref, pid) do
      [{ref, 1}, {:my_id, 2}, {pid, 3}]
    end

    setup do
      Manager.start(SimplePipeline)
    end

    test "run stream and check events" do
      ref = make_ref()
      pid = self()

      result =
        sample_stream_with_ids(ref, pid)
        |> Manager.steam_with_ids_to(SimplePipeline)
        |> Enum.to_list()

      assert [{^ref, 4}, {:my_id, 6}, {^pid, 8}] = result
    end

    test "run stream with return_ips: true option" do
      ref = make_ref()
      pid = self()

      result =
        sample_stream_with_ids(ref, pid)
        |> Manager.steam_with_ids_to(SimplePipeline, %{return_ips: true})
        |> Enum.to_list()

      assert [
               {^ref, %IP{ref: ^ref, event: 4}},
               {:my_id, %IP{ref: :my_id, event: 6}},
               {^pid, %IP{ref: ^pid, event: 8}}
             ] = result
    end
  end

  describe "components/1" do
    setup do
      Manager.start(SimplePipeline)
    end

    test "get components module" do
      components = Manager.components(SimplePipeline)
      assert length(components) == 4
    end
  end

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

  describe "add_component" do
    setup do
      Manager.start(PipelineToScale)
      on_exit(fn -> Manager.stop(PipelineToScale) end)
      %{components: Manager.components(PipelineToScale)}
    end

    test "add_component to add_one and then to mult_two", %{components: init_components} do
      component = Enum.find(init_components, &(&1.name == :add_one))
      Manager.add_component(PipelineToScale, component.stage_set_ref)

      components = Manager.components(PipelineToScale)

      producer = Enum.find(components, &(&1.name == :producer))
      [add_one1, add_one2] = Enum.filter(components, &(&1.name == :add_one))

      subscriber_pids = Enum.map(producer.subscribers, fn {pid, _ref} -> pid end)
      assert Enum.count(producer.subscribers) == 2
      assert Enum.member?(subscriber_pids, add_one1.pid)
      assert Enum.member?(subscriber_pids, add_one2.pid)

      assert add_one1.subscribe_to == [{producer.pid, [max_demand: 1, cancel: :transient]}]
      assert add_one2.subscribe_to == [{producer.pid, [max_demand: 1, cancel: :transient]}]

      mult_two_component = Enum.find(components, &(&1.name == :mult_two))

      assert Enum.count(mult_two_component.subscribe_to) == 2

      assert Enum.member?(
               mult_two_component.subscribe_to,
               {add_one1.pid, [max_demand: 1, cancel: :transient]}
             )

      assert Enum.member?(
               mult_two_component.subscribe_to,
               {add_one2.pid, [max_demand: 1, cancel: :transient]}
             )

      assert add_one1.count == 2
      assert add_one2.count == 2
      assert add_one1.stage_set_ref == add_one2.stage_set_ref
      assert abs(add_one1.number - add_one2.number) == 1

      #      # Add to mult_two
      Manager.add_component(PipelineToScale, mult_two_component.stage_set_ref)
      components = Manager.components(PipelineToScale)

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

      assert Enum.member?(
               mult_two1.subscribe_to,
               {add_one1.pid, [max_demand: 1, cancel: :transient]}
             )

      assert Enum.member?(
               mult_two1.subscribe_to,
               {add_one2.pid, [max_demand: 1, cancel: :transient]}
             )

      assert Enum.member?(
               mult_two2.subscribe_to,
               {add_one1.pid, [max_demand: 1, cancel: :transient]}
             )

      assert Enum.member?(
               mult_two2.subscribe_to,
               {add_one2.pid, [max_demand: 1, cancel: :transient]}
             )

      assert mult_two1.count == 2
      assert mult_two2.count == 2
      assert abs(mult_two1.number - mult_two2.number) == 1

      consumer_pid = consumer.pid
      assert [{^consumer_pid, _ref}] = mult_two1.subscribers
      assert [{^consumer_pid, _ref}] = mult_two2.subscribers

      assert Enum.member?(
               consumer.subscribe_to,
               {mult_two1.pid, [max_demand: 1, cancel: :transient]}
             )

      assert Enum.member?(
               consumer.subscribe_to,
               {mult_two2.pid, [max_demand: 1, cancel: :transient]}
             )
    end

    test "if components states are identical", %{components: init_components} do
      component = Enum.find(init_components, &(&1.name == :add_one))
      Manager.add_component(PipelineToScale, component.stage_set_ref)

      PipelineToScale
      |> Manager.components()
      |> Enum.each(fn component ->
        assert component == component.__struct__.__state__(component.pid)
      end)

      component = Enum.find(Manager.components(PipelineToScale), &(&1.name == :mult_two))
      Manager.add_component(PipelineToScale, component.stage_set_ref)

      PipelineToScale
      |> Manager.components()
      |> Enum.each(fn component ->
        assert component == component.__struct__.__state__(component.pid)
      end)
    end
  end

  describe "remove_component" do
    setup do
      Manager.start(PipelineToScale)
      init_components = Manager.components(PipelineToScale)
      component = Enum.find(init_components, &(&1.name == :add_one))
      Manager.add_component(PipelineToScale, component.stage_set_ref)

      component = Enum.find(Manager.components(PipelineToScale), &(&1.name == :mult_two))
      Manager.add_component(PipelineToScale, component.stage_set_ref)

      on_exit(fn -> Manager.stop(PipelineToScale) end)
      %{components: Manager.components(PipelineToScale)}
    end

    test "remove add_one and then to mult_two", %{components: init_components} do
      component = Enum.find(init_components, &(&1.name == :add_one))
      Manager.remove_component(PipelineToScale, component.stage_set_ref)

      components = Manager.components(PipelineToScale)

      producer = Enum.find(components, &(&1.name == :producer))
      [add_one] = Enum.filter(components, &(&1.name == :add_one))
      [mult_two1, mult_two2] = Enum.filter(components, &(&1.name == :mult_two))

      add_one_pid = add_one.pid
      assert [{^add_one_pid, _ref}] = producer.subscribers
      assert add_one.count == 1
      assert add_one.number == 0
      producer_pid = producer.pid
      assert [{^producer_pid, [max_demand: 1, cancel: :transient]}] = add_one.subscribe_to

      subscriber_pids = Enum.map(add_one.subscribers, fn {pid, _ref} -> pid end)
      assert Enum.member?(subscriber_pids, mult_two1.pid)
      assert Enum.member?(subscriber_pids, mult_two2.pid)

      assert [{^add_one_pid, [max_demand: 1, cancel: :transient]}] = mult_two1.subscribe_to
      assert [{^add_one_pid, [max_demand: 1, cancel: :transient]}] = mult_two2.subscribe_to

      # remove mult_two
      component = Enum.find(components, &(&1.name == :mult_two))
      # TODO weird thing with
      # [error] GenStage consumer #PID<0.377.0> received $gen_producer message: {:"$gen_producer", {#PID<0.372.0>, #Reference<0.4115812848.3137077253.82677>},
      # {:cancel, :shutdown}}
      # should be addressed later
      capture_log(fn ->
        Manager.remove_component(PipelineToScale, component.stage_set_ref)
      end)

      components = Manager.components(PipelineToScale)

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
      assert [{^add_one_pid, [max_demand: 1, cancel: :transient]}] = mult_two.subscribe_to

      assert [{^mult_two_pid, [max_demand: 1, cancel: :transient]}] = consumer.subscribe_to

      consumer_pid = consumer.pid
      assert [{^consumer_pid, _ref}] = mult_two.subscribers

      # try to remove one more
      assert {:error, :only_one_left} =
               Manager.remove_component(PipelineToScale, component.stage_set_ref)
    end

    test "if components states are identical", %{components: init_components} do
      component = Enum.find(init_components, &(&1.name == :add_one))
      Manager.remove_component(PipelineToScale, component.stage_set_ref)

      PipelineToScale
      |> Manager.components()
      |> Enum.each(fn component ->
        assert component == component.__struct__.__state__(component.pid)
      end)

      component = Enum.find(Manager.components(PipelineToScale), &(&1.name == :mult_two))
      # TODO weird thing with
      # [error] GenStage consumer #PID<0.377.0> received $gen_producer message: {:"$gen_producer", {#PID<0.372.0>, #Reference<0.4115812848.3137077253.82677>},
      # {:cancel, :shutdown}}
      # should be addressed later
      capture_log(fn ->
        Manager.remove_component(PipelineToScale, component.stage_set_ref)
      end)

      PipelineToScale
      |> Manager.components()
      |> Enum.each(fn component ->
        assert component == component.__struct__.__state__(component.pid)
      end)
    end

    test "there is no lost ips after stopping" do
      stream1 = Manager.stream_to(0..99, PipelineToScale)
      stream2 = Manager.stream_to(100..199, PipelineToScale)

      [task1, task2] =
        [stream1, stream2]
        |> Enum.map(fn stream ->
          Task.async(fn -> Enum.to_list(stream) end)
        end)

      Process.sleep(10)
      component = Enum.find(Manager.components(PipelineToScale), &(&1.name == :add_one))
      Manager.remove_component(PipelineToScale, component.stage_set_ref)

      assert Task.await(task1) -- Enum.map(0..99, fn n -> (n + 1) * 2 end) == []
      assert Task.await(task2) -- Enum.map(100..199, fn n -> (n + 1) * 2 end) == []
    end
  end


  describe "remove_component 2" do
    defmodule PipelineToScale2 do
      use ALF.DSL

      @components [
        stage(:add_one, count: 2),
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

      on_exit(fn -> Manager.stop(PipelineToScale2) end)
      %{components: Manager.components(PipelineToScale2)}
    end

#    test "remove add_one and then to mult_two", %{components: init_components} do
#      IO.inspect(init_components)
#      component = Enum.find(init_components, &(&1.name == :add_one))
#      Manager.remove_component(PipelineToScale2, component.stage_set_ref)
#
#      Process.sleep(100)
#
#    end
  end
end
