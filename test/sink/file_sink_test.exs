defmodule ALF.Sink.FileSinkTest do
  use ExUnit.Case, async: true

  alias ALF.Sink.FileSink
  alias ALF.Source.FileSource

  test "Sink" do
    source = FileSource.open("test/sink/orders.csv")
    sink = FileSink.open("test/sink/output.csv")

    source
    |> FileSource.stream()
    |> FileSink.stream(sink)
    |> Stream.run()

    assert File.read!("test/sink/orders.csv") <> "\n" == File.read!("test/sink/output.csv")
  end
end
