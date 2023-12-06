defmodule ALF.MixStreams.Pipeline do
  use ALF.DSL

  @components [
    composer(:sum, memo: 0)
  ]

  def sum(:end, :done, _) do
    {[], :done}
  end

  def sum(:end, memo, _) do
    {[memo], :done}
  end

  def sum(event, memo, _) do
    {[], event + memo}
  end
end

defmodule ALF.MixStreamsTest do
  use ExUnit.Case, async: true

  alias ALF.MixStreams.Pipeline

  setup do
    Pipeline.start()
    on_exit(&Pipeline.stop/0)
  end

  test "several streams of inputs" do
    stream1 = Pipeline.stream(Enum.to_list(0..9) ++ [:end])
    stream2 = Pipeline.stream(Enum.to_list(10..19) ++ [:end])
    stream3 = Pipeline.stream(Enum.to_list(20..29) ++ [:end])

    assert [[45], [145], [245]] =
             [stream1, stream2, stream3]
             |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
             |> Task.await_many()
  end

  test "with an explicit stream_ref" do
    #    stream1 = Pipeline.stream(Enum.to_list(0..9) ++ [:end], stream_ref: :the_stream)
    #    stream2 = Pipeline.stream(Enum.to_list(10..19) ++ [:end], stream_ref: :the_stream)
    #    stream3 = Pipeline.stream(Enum.to_list(20..29) ++ [:end], stream_ref: :the_stream)
    #    stream4 = Pipeline.stream(Enum.to_list(30..39) ++ [:end])
    #
    #    [[sum1], [sum2], [sum3], [345]] =
    #      [stream1, stream2, stream3, stream4]
    #      |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
    #      |> Task.await_many()
    #
    #    assert sum1 + sum2 + sum3 == 435
  end
end
