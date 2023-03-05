defmodule ALF.SyncRun.DecomposeRecomposeTest do
  use ExUnit.Case, async: false

  describe "decompose an recompose" do
    defmodule Pipeline do
      use ALF.DSL

      @components [
        decomposer(:decomposer_function),
        recomposer(:recomposer_function)
      ]

      def decomposer_function(event, _) do
        String.split(event)
      end

      def recomposer_function(event, prev_events, _) do
        string = Enum.join(prev_events ++ [event], " ")

        if String.length(string) > 10 do
          string
        else
          :continue
        end
      end
    end

    setup do
      Pipeline.start(sync: true)
      on_exit(&Pipeline.stop/0)
    end

    test "returns strings" do
      [event1, event2] =
        ["foo foo", "bar bar", "baz baz"]
        |> Pipeline.stream()
        |> Enum.to_list()

      assert event1 == "foo foo bar"
      assert event2 == "bar baz baz"
    end

    test "several streams returns strings" do
      stream1 = Pipeline.stream(["foo foo", "bar bar", "baz baz"])
      stream2 = Pipeline.stream(["foo foo", "bar bar", "baz baz"])
      stream3 = Pipeline.stream(["foo foo", "bar bar", "baz baz"])

      [result1, result2, result3] =
        [stream1, stream2, stream3]
        |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
        |> Task.await_many()

      assert ^result1 = ^result2 = ^result3 = ["foo foo bar", "bar baz baz"]
    end
  end
end
