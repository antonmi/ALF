defmodule ALF.SyncRun.ComposeTest do
  use ExUnit.Case, async: true

  describe "decompose an recompose" do
    defmodule Pipeline do
      use ALF.DSL

      @components [
        composer(:decomposer_function),
        composer(:recomposer_function, acc: [])
      ]

      def decomposer_function(event, nil, _) do
        {String.split(event), nil}
      end

      def recomposer_function(event, prev_events, _) do
        string = Enum.join(prev_events ++ [event], " ")

        if String.length(string) > 10 do
          {[string], []}
        else
          {[], prev_events ++ [event]}
        end
      end
    end

    setup do
      Pipeline.start(sync: true)
      on_exit(&Pipeline.stop/0)
    end

    test "returns strings" do
      [ip1, ip2] =
        ["foo foo", "bar bar", "baz baz"]
        |> Pipeline.stream(debug: true)
        |> Enum.to_list()

      assert ip1.event == "foo foo bar"
      assert ip2.event == "bar baz baz"

      assert ip1.history == [
               {{:recomposer_function, 0}, "bar"},
               {{:decomposer_function, 0}, "bar bar"}
             ]
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
        composer(:split_to_words),
        composer(:create_lines, acc: [])
      ]

      @length_limit 10

      def split_to_words(line, nil, _) do
        {String.split(String.trim(line)), nil}
      end

      def create_lines(word, words, _) do
        string_before = Enum.join(words, " ")
        string_after = Enum.join(words ++ [word], " ")

        cond do
          String.length(string_after) == @length_limit ->
            {[string_after], []}

          String.length(string_after) > @length_limit ->
            {[string_before], [word]}

          true ->
            {[], words ++ [word]}
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
