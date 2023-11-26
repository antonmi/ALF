defmodule ALF.Source.FileSourceTest do
  use ExUnit.Case, async: true

  alias ALF.Source.FileSource

  test "FileSource" do
    path = "test/source/orders.csv"
    source1 = FileSource.open(path, chunk_size: 1)
    source2 = FileSource.open(path, chunk_size: 5)

    lines1 =
      source1
      |> FileSource.stream()
      |> Enum.into([])

    lines2 =
      source2
      |> FileSource.stream()
      |> Enum.into([])

    length =
      path
      |> File.read!()
      |> String.split("\n")
      |> length()

    assert length(lines1) == length
    assert length(lines2) == length
  end
end
