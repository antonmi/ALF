defmodule ALF.Examples.Telegram.Pipeline do
  use ALF.DSL

  @components [
    decomposer(:split_to_words, function: :split_to_words),
    recomposer(:create_lines, function: :create_lines),
  ]

  @length_limit 50

  def split_to_words(line, _opts) do
    line
    |> String.trim()
    |> String.split()
  end

  def create_lines(word, words, _opts) do
    string = Enum.join(words, " ")
    if String.length(string <> " " <> word) > @length_limit do
      string
    else
      :continue
    end
  end

end

defmodule ALF.Examples.TelegramTest do
  use ExUnit.Case

  alias ALF.Examples.Telegram.Pipeline
  alias ALF.Manager

  setup do
    file_stream = File.stream!("test/examples/telegram_input.txt")
    Manager.start(Pipeline)
    %{file_stream: file_stream}
  end

  test "process input", %{file_stream: file_stream} do
    lines =
      file_stream
      |> Manager.stream_to(Pipeline)
      |> Enum.to_list()

    assert Enum.count(lines) > 100
    assert String.length(hd(lines)) <= 50
  end
end
