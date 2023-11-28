defmodule ALF.Source.FileSourceTest do
  use ExUnit.Case, async: true

  alias ALF.Source.FileSource

  test "FileSource" do
    path = "test/source/orders.csv"
    source1 = FileSource.open(path, chars: 1)
    source2 = FileSource.open(path, chars: 5)

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

  test "batch mode" do
    path = "test/source/parcels.csv"
    source = FileSource.open(path, chars: 10_000, batch_size: 10)

    batches =
      source
      |> FileSource.stream()
      |> Enum.into([])

    assert length(batches) == 14
    assert length(hd(batches)) == 10
  end
end
