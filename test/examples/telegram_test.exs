defmodule ALF.Examples.Telegram.Pipeline do
  use ALF.DSL

  @components [
    decomposer(:split_to_words),
    recomposer(:create_lines)
  ]

  @length_limit 50

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

defmodule ALF.Examples.TelegramTest do
  use ExUnit.Case

  alias ALF.Examples.Telegram.Pipeline

  setup do
    file_stream = File.stream!("test/examples/telegram_input.txt")
    Pipeline.start()
    on_exit(fn -> Pipeline.stop() end)
    %{file_stream: file_stream}
  end

  test "process input", %{file_stream: file_stream} do
    lines =
      file_stream
      |> Pipeline.stream()
      |> Enum.to_list()

    assert Enum.count(lines) > 100
    assert String.length(hd(lines)) <= 50
  end
end
