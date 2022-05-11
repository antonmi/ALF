defmodule ALF.DoneTest do
  use ExUnit.Case, async: false

  alias ALF.{Manager, IP}

  describe "done! in stage" do
    defmodule DoneInStagePipeline do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _), do: done!(event + 1)
      def mult_two(event, _), do: event * 2
    end

    setup do
      Manager.start(DoneInStagePipeline)
    end

    test "returns result immediately (skips mult_two)" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(DoneInStagePipeline, return_ips: true)
        |> Enum.to_list()

      assert [
               %IP{
                 event: 2,
                 history: [{{:add_one, 0}, 1}]
               },
               %IP{},
               %IP{}
             ] = results
    end

    test "without the return_ips option" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(DoneInStagePipeline)
        |> Enum.to_list()

      assert results == [2, 3, 4]
    end
  end

  describe "done! in stage before consumer" do
    defmodule DoneInLastStagePipeline do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _), do: event + 1
      def mult_two(event, _), do: done!(event * 2)
    end

    setup do
      Manager.start(DoneInLastStagePipeline)
    end

    test "returns result immediately (skips mult_two)" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(DoneInLastStagePipeline, return_ips: true)
        |> Enum.to_list()

      assert [
               %IP{
                 event: 4,
                 history: [{{:mult_two, 0}, 2}, {{:add_one, 0}, 1}]
               },
               %IP{},
               %IP{}
             ] = results
    end

    test "without the return_ips option" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(DoneInLastStagePipeline)
        |> Enum.to_list()

      assert results == [4, 6, 8]
    end
  end
end
