defmodule ALF.Examples.TelegramWithComposer.Pipeline do
  use ALF.DSL

  @components [
    composer(:split_to_words),
    composer(:create_lines, acc: [])
  ]

  @length_limit 50

  def split_to_words(line, nil, _) do
    words =
      line
      |> String.trim()
      |> String.split()

    {words, nil}
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

defmodule ALF.Examples.TelegramWithComposerTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.TelegramWithComposer.Pipeline

  setup do
    file_stream = File.stream!("test/examples/telegram_input.txt")
    Pipeline.start()
    on_exit(&Pipeline.stop/0)
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
