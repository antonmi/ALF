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
  use ExUnit.Case

  alias ALF.SyncRun.SimplePipeline.Pipeline
  alias ALF.Manager

  setup do: Manager.start(Pipeline, sync: true)

  test "sync run" do
    results =
      [1, 2, 3]
      |> Manager.stream_to(Pipeline)
      |> Enum.to_list()

    assert results == [1, 3, 5]
  end

  test "several streams of inputs" do
    stream1 = Manager.stream_to(0..9, Pipeline)
    stream2 = Manager.stream_to(10..19, Pipeline)
    stream3 = Manager.stream_to(20..29, Pipeline)

    [result1, result2, result3] =
      [stream1, stream2, stream3]
      |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
      |> Task.await_many()

    assert result1 == Enum.map(0..9, fn n -> (n + 1) * 2 - 3 end)
    assert result2 == Enum.map(10..19, fn n -> (n + 1) * 2 - 3 end)
    assert result3 == Enum.map(20..29, fn n -> (n + 1) * 2 - 3 end)
  end
end
