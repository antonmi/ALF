defmodule ALF.SyncRun.SimplePipeline.Pipeline do
  use ALF.DSL

  @components [
    stage(:add_one),
    stage(:mult_by_two),
    stage(:minus_three)
  ]

  def add_one(event, _), do: event + 1
  def mult_by_two(event, _), do: event * 2
  def minus_three(event, _), do: event - 3
end

defmodule ALF.SyncRun.SimplePipelineTest do
  use ExUnit.Case, async: true

  alias ALF.SyncRun.SimplePipeline.Pipeline
  alias ALF.IP

  setup do
    Pipeline.start(sync: true)
  end

  test "sync run" do
    results =
      [1, 2, 3]
      |> Pipeline.stream()
      |> Enum.to_list()

    assert results == [1, 3, 5]
  end

  test "several streams of inputs" do
    stream1 = Pipeline.stream(0..9)
    stream2 = Pipeline.stream(10..19)
    stream3 = Pipeline.stream(20..29)

    [result1, result2, result3] =
      [stream1, stream2, stream3]
      |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
      |> Task.await_many()

    assert result1 == Enum.map(0..9, fn n -> (n + 1) * 2 - 3 end)
    assert result2 == Enum.map(10..19, fn n -> (n + 1) * 2 - 3 end)
    assert result3 == Enum.map(20..29, fn n -> (n + 1) * 2 - 3 end)
  end

  test "debug true" do
    results =
      [1, 2, 3]
      |> Pipeline.stream(debug: true)
      |> Enum.to_list()

    assert [%IP{event: 1}, %IP{event: 3}, %IP{event: 5}] = results
  end
end
