defmodule ALF.ManagerTest do
  use ExUnit.Case, async: true

  alias ALF.Manager

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(:add_one),
      stage(:mult_two)
    ]

    def add_one(datum, _opts), do: datum + 1
    def mult_two(datum, _opts), do: datum * 2
  end

  defmodule GoToPipeline do
    use ALF.DSL

    @components [
      goto_point(:point),
      goto(:goto, to: :point, if: :goto_if)
    ]
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
      assert add.subscribe_to == [{producer.pid, [max_demand: 1]}]
      mult_pid = mult.pid
      assert [{^mult_pid, _ref}] = add.subscribers

      assert mult.name == :mult_two
      assert mult.subscribe_to == [{add.pid, [max_demand: 1]}]
      consumer_pid = consumer.pid
      assert [{^consumer_pid, _ref}] = mult.subscribers

      assert consumer.name == :consumer
      assert consumer.subscribe_to == [{mult.pid, [max_demand: 1]}]
    end
  end

  describe "prepare gotos after initialization" do
    setup do
      Manager.start(GoToPipeline)
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

    setup do: Manager.start(SimplePipeline)

    test "run stream and check data" do
      results =
        sample_stream()
        |> Manager.stream_to(SimplePipeline)
        |> Enum.to_list()

      assert results == [4, 6, 8]
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
               %ALF.IP{datum: 4},
               %ALF.IP{datum: 6},
               %ALF.IP{datum: 8}
             ] = results
    end
  end

  describe "graph_edges/1" do
    setup do
      :ok = Manager.start(GoToPipeline)
      %{edges: Manager.graph_edges(GoToPipeline)}
    end

    test "check edges", %{edges: edges} do
      assert Enum.member?(
               edges,
               {"ALF.ManagerTest.GoToPipeline-producer", "ALF.ManagerTest.GoToPipeline-point"}
             )

      assert Enum.member?(
               edges,
               {"ALF.ManagerTest.GoToPipeline-point", "ALF.ManagerTest.GoToPipeline-goto"}
             )

      assert Enum.member?(
               edges,
               {"ALF.ManagerTest.GoToPipeline-goto", "ALF.ManagerTest.GoToPipeline-consumer"}
             )

      assert Enum.member?(
               edges,
               {"ALF.ManagerTest.GoToPipeline-goto", "ALF.ManagerTest.GoToPipeline-point"}
             )
    end
  end
end
