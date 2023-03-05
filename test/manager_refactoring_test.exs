defmodule ALF.ManagerRefactoringTest do
  use ExUnit.Case, async: false

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
      SimplePipelineToStream.start()
      on_exit(&SimplePipelineToStream.stop/0)
    end

    test "stream" do
      results =
        @sample_stream
        |> SimplePipelineToStream.stream()
        |> Enum.to_list()

      assert results == [4, 6, 8]
    end

    test "stream with return_ips option" do
      assert [%ALF.IP{event: 4}, %ALF.IP{event: 6}, %ALF.IP{event: 8}] =
               @sample_stream
               |> SimplePipelineToStream.stream(return_ips: true)
               |> Enum.to_list()
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
end
