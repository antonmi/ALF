defmodule ALF.Examples.Stream.MergePipeline do
  use ALF.DSL

  @components [
    composer(:merge, count: 3)
  ]

  def merge(stream, nil, _) do
    {Enum.into(stream, []), nil}
  end
end

defmodule ALF.Examples.Stream.MergeTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.Stream.MergePipeline

  describe "for async pipeline" do
    setup do
      MergePipeline.start()
      on_exit(&MergePipeline.stop/0)
    end

    test "merge" do
      results =
        [1..3, 4..6, 7..9]
        |> MergePipeline.stream()
        |> Enum.to_list()

      assert Enum.sort(results) == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    end
  end

  describe "for sync pipeline" do
    setup do
      MergePipeline.start(sync: true)
      on_exit(&MergePipeline.stop/0)
    end

    test "merge" do
      results =
        [1..3, 4..6, 7..9]
        |> MergePipeline.stream()
        |> Enum.to_list()

      assert results == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    end
  end
end
