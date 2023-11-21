defmodule ALF.Examples.Stream.ConcatPipeline do
  use ALF.DSL

  @components [
    composer(:concat)
  ]

  def concat(events, nil, _) do
    {Enum.into(events, []), nil}
  end
end

defmodule ALF.Examples.Stream.ConcatTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.Stream.ConcatPipeline

  describe "for async pipeline" do
    setup do
      ConcatPipeline.start()
      on_exit(&ConcatPipeline.stop/0)
    end

    test "concat" do
      results =
        [1..3, 4..6, 7..9]
        |> ConcatPipeline.stream()
        |> Enum.to_list()

      assert Enum.sort(results) == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    end
  end

  describe "for sync pipeline" do
    setup do
      ConcatPipeline.start(sync: true)
      on_exit(&ConcatPipeline.stop/0)
    end

    test "concat" do
      results =
        [1..3, 4..6, 7..9]
        |> ConcatPipeline.stream()
        |> Enum.to_list()

      assert results == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    end
  end
end
