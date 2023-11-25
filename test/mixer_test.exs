defmodule ALF.Sink.MixerTest do
  use ExUnit.Case, async: false

  alias ALF.Source.FileSource
  alias ALF.Mixer

  def orders_and_parcels do
    orders =
      "test/source/orders.csv"
      |> File.read!()
      |> String.split("\n")

    parcels =
      "test/source/parcels.csv"
      |> File.read!()
      |> String.split("\n")

    {orders, parcels}
  end

  setup do
    source1 = FileSource.open("test/source/orders.csv")
    source2 = FileSource.open("test/source/parcels.csv")

    stream1 = FileSource.stream(source1)
    stream2 = FileSource.stream(source2)

    %{stream1: stream1, stream2: stream2}
  end

  test "stream", %{stream1: stream1, stream2: stream2} do
    lines =
      [stream1, stream2]
      |> Mixer.new()
      |> Mixer.stream()
      |> Enum.into([])

    {orders, parcels} = orders_and_parcels()
    assert lines -- (orders ++ parcels) == []
    assert (orders ++ parcels) -- lines == []
  end

  test "stream two identical streams", %{stream1: stream1} do
    lines =
      [stream1, stream1]
      |> Mixer.new()
      |> Mixer.stream()
      |> Enum.into([])

    {orders, _parcels} = orders_and_parcels()
    assert lines -- orders == []
    assert orders -- lines == []
  end

  test "stream two identical files", %{stream1: stream1} do
    source2 = FileSource.open("test/source/orders.csv")
    stream2 = FileSource.stream(source2)

    lines =
      [stream1, stream2]
      |> Mixer.new()
      |> Mixer.stream()
      |> Enum.into([])

    {orders, _parcels} = orders_and_parcels()
    assert lines -- (orders ++ orders) == []
    assert (orders ++ orders) -- lines == []
  end

  test "add streams", %{stream1: stream1, stream2: stream2} do
    lines =
      stream1
      |> Mixer.new()
      |> Mixer.add(stream2)
      |> Mixer.stream()
      |> Enum.into([])

    {orders, parcels} = orders_and_parcels()
    assert lines -- (orders ++ parcels) == []
    assert (orders ++ parcels) -- lines == []
  end

  test "dynamically add streams", %{stream1: stream1, stream2: stream2} do
    mixer = Mixer.new(stream1)

    Task.async(fn ->
      Mixer.add(mixer, stream2)
    end)

    lines =
      mixer
      |> Mixer.stream()
      |> Enum.into([])

    {orders, parcels} = orders_and_parcels()
    assert lines -- (orders ++ parcels) == []
    assert (orders ++ parcels) -- lines == []
  end

#    @tag timeout: 300_000
#    test "memory" do
##      add :observer, :runtime_tools, :wx to extra_applications
#      :observer.start()
#
#      source1 = FileSource.open("test_data/input.csv")
#      source2 = FileSource.open("test_data/input.csv")
#
#      stream1 = FileSource.stream(source1)
#      stream2 = FileSource.stream(source2)
#      sink = ALF.Sink.FileSink.open("test_data/output.csv")
#
#      stream1
#      |> Mixer.new()
#      |> Mixer.add(stream2)
#      |> Mixer.stream()
#      |> ALF.Sink.FileSink.stream(sink)
#      |> Stream.run()
#    end
end
