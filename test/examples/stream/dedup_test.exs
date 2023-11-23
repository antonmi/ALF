defmodule ALF.Examples.Stream.DedupPipeline do
  use ALF.DSL

  @components [
    composer(:dedup)
  ]

  def dedup(event, prev_event, _) do
    if event != prev_event do
      {[event], event}
    else
      {[], event}
    end
  end
end

defmodule ALF.Examples.Stream.DedupTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.Stream.DedupPipeline

  describe "for async pipeline" do
    setup do
      DedupPipeline.start()
      on_exit(&DedupPipeline.stop/0)
    end

    test "dedup" do
      results =
        [1, 2, 3, 3, 2, 1]
        |> DedupPipeline.stream()
        |> Enum.to_list()

      assert results == [1, 2, 3, 2, 1]
    end
  end

  describe "for sync pipeline" do
    setup do
      DedupPipeline.start(sync: true)
      on_exit(&DedupPipeline.stop/0)
    end

    test "dedup" do
      results =
        [1, 2, 3, 3, 2, 1]
        |> DedupPipeline.stream()
        |> Enum.to_list()

      assert results == [1, 2, 3, 2, 1]
    end
  end
end
