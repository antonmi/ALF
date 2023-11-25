defmodule ALF.Examples.ComposerWithStreamsTest do
  use ExUnit.Case, async: true

  defmodule Pipeline do
    use ALF.DSL

    @components [
      composer(:sum_and_emit, memo: 0)
    ]

    def sum_and_emit(event, memo, _) do
      sum = event + memo

      if sum > 5 do
        {[sum], 0}
      else
        {[], event + memo}
      end
    end
  end

  setup do
    Pipeline.start(sync: true)
    on_exit(&Pipeline.stop/0)
  end

  test "one stream" do
    results =
      [1, 2, 3, 4, 5]
      |> Pipeline.stream()
      |> Enum.to_list()

    assert results == [6, 9]
  end

  test "several streams" do
    stream1 = Pipeline.stream(1..5)
    stream2 = Pipeline.stream(1..5)
    stream3 = Pipeline.stream(1..5)

    [result1, result2, result3] =
      [stream1, stream2, stream3]
      |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
      |> Task.await_many()

    assert [result1, result2, result3] == [[6, 9], [6, 9], [6, 9]]
  end
end
