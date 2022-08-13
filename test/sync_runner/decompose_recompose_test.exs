defmodule ALF.SyncRunner.DecomposeRecomposeTest do
  use ExUnit.Case, async: false

  alias ALF.{Manager, SyncRunner}

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
      Manager.start(Pipeline)
    end

    test "returns strings" do
      [event1, event2] =
        ["foo foo", "bar bar", "baz baz"]
        |> SyncRunner.stream_to(Pipeline)
        |> Enum.to_list()

      assert event1 == "foo foo bar"
      assert event2 == "bar baz baz"
    end
  end
end
