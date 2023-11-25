defmodule ALF.Examples.Stream.ChunkByPipeline do
  use ALF.DSL

  defmodule ChunkBy do
    def call(number) do
      rem(number, 2) == 1
    end
  end

  @components [
    composer(:chunk_by, memo: {nil, []}, opts: %{fun: &ChunkBy.call/1})
  ]

  def chunk_by(:end, acc, _opts) do
    {_prev_flag, events} = acc
    {[Enum.reverse(events)], {nil, []}}
  end

  def chunk_by(event, {nil, []}, opts), do: {[], {opts[:fun].(event), [event]}}

  def chunk_by(event, acc, opts) do
    {prev_flag, events} = acc
    flag = opts[:fun].(event)

    if flag != prev_flag do
      {[Enum.reverse(events)], {flag, [event]}}
    else
      {[], {flag, [event | events]}}
    end
  end
end

defmodule ALF.Examples.Stream.ChunkByTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.Stream.ChunkByPipeline

  describe "for async pipeline" do
    setup do
      ChunkByPipeline.start()
      on_exit(&ChunkByPipeline.stop/0)
    end

    test "chunk_by" do
      results =
        [1, 2, 2, 3, 4, 4, 6, 7, 7, :end]
        |> ChunkByPipeline.stream()
        |> Enum.to_list()

      assert results == [[1], [2, 2], [3], [4, 4, 6], [7, 7]]
    end
  end

  describe "for sync pipeline" do
    setup do
      ChunkByPipeline.start(sync: true)
      on_exit(&ChunkByPipeline.stop/0)
    end

    test "chunk_by" do
      results =
        [1, 1, 2, 2, 3, 4, 4, 6, 7, 7, :end]
        |> ChunkByPipeline.stream()
        |> Enum.to_list()

      assert results == [[1, 1], [2, 2], [3], [4, 4, 6], [7, 7]]
    end
  end
end
