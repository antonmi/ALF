defmodule ALF.DoneTest do
  use ExUnit.Case, async: true

  describe "done" do
    defmodule DoneInStagePipeline do
      use ALF.DSL

      @components [
        stage(:add_one),
        done(:event_is_big_enough),
        stage(:mult_two)
      ]

      def add_one(event, _), do: event + 1
      def event_is_big_enough(event, _), do: event > 2
      def mult_two(event, _), do: event * 2
    end

    setup do
      DoneInStagePipeline.start()
      on_exit(&DoneInStagePipeline.stop/0)
    end

    test "returns some results immediately (skips mult_two)" do
      results =
        [1, 2, 3]
        |> DoneInStagePipeline.stream()
        |> Enum.to_list()

      assert results == [4, 3, 4]
    end
  end

  describe "done when error in condition" do
    defmodule DoneWithErrorInStagePipeline do
      use ALF.DSL

      @components [
        stage(:add_one),
        done(:event_is_big_enough),
        stage(:mult_two)
      ]

      def add_one(event, _), do: event + 1
      def event_is_big_enough(event, _), do: if(event > 2, do: raise("error"))
      def mult_two(event, _), do: event * 2
    end

    setup do
      DoneWithErrorInStagePipeline.start()
      on_exit(&DoneWithErrorInStagePipeline.stop/0)
    end

    test "it returns error" do
      results =
        [1, 2, 3]
        |> DoneWithErrorInStagePipeline.stream()
        |> Enum.to_list()

      assert [
               4,
               %ALF.ErrorIP{error: %RuntimeError{message: "error"}},
               %ALF.ErrorIP{error: %RuntimeError{message: "error"}}
             ] = results
    end
  end

  describe "done in sync pipeline" do
    defmodule DoneInStagePipelineSync do
      use ALF.DSL

      @components [
        stage(:add_one),
        done(:event_is_big_enough),
        stage(:mult_two)
      ]

      def add_one(event, _), do: event + 1
      def event_is_big_enough(event, _), do: event > 2
      def mult_two(event, _), do: event * 2
    end

    setup do
      DoneInStagePipelineSync.start(sync: true)
      on_exit(&DoneInStagePipelineSync.stop/0)
    end

    test "returns some results immediately (skips mult_two)" do
      results =
        [1, 2, 3]
        |> DoneInStagePipelineSync.stream()
        |> Enum.to_list()

      assert results == [4, 3, 4]
    end
  end

  describe "done in sync pipeline when error in condition" do
    defmodule DoneWithErrorInStagePipelineSync do
      use ALF.DSL

      @components [
        stage(:add_one),
        done(:event_is_big_enough),
        stage(:mult_two)
      ]

      def add_one(event, _), do: event + 1
      def event_is_big_enough(event, _), do: if(event > 2, do: raise("error"))
      def mult_two(event, _), do: event * 2
    end

    setup do
      DoneWithErrorInStagePipelineSync.start(sync: true)
      on_exit(&DoneWithErrorInStagePipelineSync.stop/0)
    end

    test "it returns error" do
      results =
        [1, 2, 3]
        |> DoneWithErrorInStagePipelineSync.stream()
        |> Enum.to_list()

      assert [
               4,
               %ALF.ErrorIP{error: %RuntimeError{message: "error"}},
               %ALF.ErrorIP{error: %RuntimeError{message: "error"}}
             ] = results
    end
  end
end
