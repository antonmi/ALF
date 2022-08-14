defmodule ALF.DecomposeRecomposeTest do
  use ExUnit.Case, async: false

  alias ALF.{Manager}

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
      [ip1, ip2] =
        ["foo foo", "bar bar", "baz baz"]
        |> Manager.stream_to(Pipeline, return_ips: true)
        |> Enum.to_list()

      assert ip1.event == "foo foo bar"
      assert ip2.event == "bar baz baz"
    end

    test "several streams returns strings" do
      stream1 = Manager.stream_to(["foo foo", "bar bar", "baz baz"], Pipeline)
      stream2 = Manager.stream_to(["foo foo", "bar bar", "baz baz"], Pipeline)
      stream3 = Manager.stream_to(["foo foo", "bar bar", "baz baz"], Pipeline)

      [result1, result2, result3] =
        [stream1, stream2, stream3]
        |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
        |> Task.await_many()

      assert ^result1 = ^result2 = ^result3 = ["foo foo bar", "bar baz baz"]
    end
  end
end
