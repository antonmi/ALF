defmodule ALF.ManagerRefactoringTest do
  use ExUnit.Case, async: false

  alias ALF.{IP, Manager, Pipeline}

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
      Manager.start(SimplePipelineToCall)
      on_exit(fn -> Manager.stop(SimplePipelineToCall) end)
    end

    test "run stream and check events" do
      assert Manager.call(1, SimplePipelineToCall) == 4
      assert Pipeline.call(1, SimplePipelineToCall) == 4
    end

    test "with return ip option" do
      assert %IP{event: 4} = Pipeline.call(1, SimplePipelineToCall, return_ip: true)
    end

    test "call from many Tasks" do
      1..10
      |> Enum.map(fn _event ->
        Task.async(fn ->
          assert Pipeline.call(1, SimplePipelineToCall) == 4
        end)
      end)
      |> Task.await_many()
    end
  end

  describe "stream/2" do
    defmodule SimplePipelineToStream do
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
      Manager.start(SimplePipelineToStream)
      on_exit(fn -> Manager.stop(SimplePipelineToStream) end)
    end

    test "stream" do
      results =
        @sample_stream
        |> Manager.stream(SimplePipelineToStream)
        |> Enum.to_list()

      assert results == [4, 6, 8]
    end

    test "stream with return_ips option" do
      assert [%IP{event: 4}, %IP{event: 6}, %IP{event: 8}] =
        @sample_stream
        |> Manager.stream(SimplePipelineToStream, return_ips: true)
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
      Manager.start(DecomposerPipeline)
      on_exit(fn -> Manager.stop(DecomposerPipeline) end)
    end

    test "stream" do
      results =
        ["aaa bbb ccc", "ddd eee", "xxx"]
        |> Manager.stream(DecomposerPipeline)
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
      Manager.start(RecomposerPipeline)
      on_exit(fn -> Manager.stop(RecomposerPipeline) end)
    end

    test "stream" do
      ["aa", "bb", "xxxxx"]
      |> Manager.stream(RecomposerPipeline)
      |> Enum.to_list()
    end
  end
end
