defmodule ALF.Examples.Stream.ChunkEveryPipeline do
  use ALF.DSL

  @components [
    composer(:chunk_every, memo: [], opts: %{count: 2})
  ]

  def chunk_every(:end, memo, _), do: {[Enum.reverse(memo)], []}

  def chunk_every(event, memo, opts) do
    memo = [event | memo]

    if length(memo) >= opts[:count] do
      {[Enum.reverse(memo)], []}
    else
      {[], memo}
    end
  end
end

defmodule ALF.Examples.Stream.ChunkEveryTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.Stream.ChunkEveryPipeline

  describe "for async pipeline" do
    setup do
      ChunkEveryPipeline.start()
      on_exit(&ChunkEveryPipeline.stop/0)
    end

    test "chunk_every" do
      results =
        [1, 2, 3, 4, 5, 6]
        |> ChunkEveryPipeline.stream()
        |> Enum.to_list()

      assert results == [[1, 2], [3, 4], [5, 6]]

      results =
        [1, 2, 3, 4, 5, 6, 7, :end]
        |> ChunkEveryPipeline.stream()
        |> Enum.to_list()

      assert results == [[1, 2], [3, 4], [5, 6], [7]]
    end
  end

  describe "for sync pipeline" do
    setup do
      ChunkEveryPipeline.start(sync: true)
      on_exit(&ChunkEveryPipeline.stop/0)
    end

    test "chunk_every" do
      results =
        [1, 2, 3, 4, 5, 6]
        |> ChunkEveryPipeline.stream()
        |> Enum.to_list()

      assert results == [[1, 2], [3, 4], [5, 6]]

      results =
        [1, 2, 3, 4, 5, 6, 7, :end]
        |> ChunkEveryPipeline.stream()
        |> Enum.to_list()

      assert results == [[1, 2], [3, 4], [5, 6], [7]]
    end
  end
end
