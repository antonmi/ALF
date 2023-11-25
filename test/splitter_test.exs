defmodule ALF.Sink.SplitterTest do
  use ExUnit.Case, async: false

  alias ALF.Source.FileSource
  alias ALF.{Mixer, Splitter}

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

  describe "Splitter" do
    setup %{stream1: stream1, stream2: stream2} do
      stream =
        [stream1, stream2]
        |> Mixer.new()
        |> Mixer.stream()

      %{stream: stream}
    end

    test "splitter", %{stream: stream} do
      {original_orders, original_parcels} = orders_and_parcels()

      partitions = [
        fn el -> String.starts_with?(el, "PARCEL_SHIPPED") end,
        fn el -> String.starts_with?(el, "ORDER_CREATED") end
      ]

      splitter = Splitter.new(stream, partitions)
      [stream1, stream2] = Splitter.stream(splitter)

      parcels = Enum.to_list(stream1)
      assert parcels -- original_parcels == []
      assert original_parcels -- parcels == []

      orders = Enum.to_list(stream2)
      assert orders -- original_orders == []
      assert original_orders -- orders == []
    end

    #    @tag timeout: 300_000
    #    test "memory" do
    #      #  add :observer, :runtime_tools, :wx to extra_applications
    #      :observer.start()
    #
    #      source = FileSource.open("test_data/input.csv")
    #      stream = FileSource.stream(source)
    #
    #      partitions = [
    #        fn el -> String.contains?(el, "Z,111,") end,
    #        fn el -> String.contains?(el, "Z,222,") end,
    #        fn el -> String.contains?(el, "Z,333,") end
    #      ]
    #
    #      splitter = Splitter.new(stream, partitions)
    #
    #      sink1 = ALF.Sink.FileSink.open("test_data/output111.csv")
    #      sink2 = ALF.Sink.FileSink.open("test_data/output222.csv")
    #      sink3 = ALF.Sink.FileSink.open("test_data/output333.csv")
    #
    #      splitter
    #      |> Splitter.stream()
    #      |> Enum.zip([sink1, sink2, sink3])
    #      |> Enum.map(fn {stream, sink} ->
    #        Task.async(fn ->
    #          stream
    #          |> ALF.Sink.FileSink.stream(sink)
    #          |> Stream.run()
    #        end)
    #      end)
    #      |> Enum.map(fn task -> Task.await(task, :infinity) end)
    #    end
  end
end
