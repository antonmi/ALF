defmodule ALF.SyncRun.DecomposeRecomposeTest do
  use ExUnit.Case, async: true

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

  describe "telegram" do
    defmodule TelegramPipeline do
      use ALF.DSL

      @components [
        decomposer(:split_to_words),
        recomposer(:create_lines)
      ]

      @length_limit 10

      def split_to_words(line, _) do
        line
        |> String.trim()
        |> String.split()
      end

      def create_lines(word, words, _) do
        string_before = Enum.join(words, " ")
        string_after = Enum.join(words ++ [word], " ")

        cond do
          String.length(string_after) == @length_limit ->
            string_after

          String.length(string_after) > @length_limit ->
            {string_before, [word]}

          true ->
            :continue
        end
      end
    end

    setup do
      TelegramPipeline.start(sync: true)
      on_exit(&TelegramPipeline.stop/0)
    end

    test "with call" do
      assert is_nil(TelegramPipeline.call("aaa"))
      assert is_nil(TelegramPipeline.call("bbb"))
      assert TelegramPipeline.call("ccc") == "aaa bbb"
      assert is_nil(TelegramPipeline.call("ddd"))
      assert TelegramPipeline.call("12345678") == "ccc ddd"
      assert TelegramPipeline.call("z") == "12345678 z"
    end
  end
end
