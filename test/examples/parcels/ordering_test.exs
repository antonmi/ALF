defmodule ALF.Examples.Ordering.Pipeline do
  use ALF.DSL

  @components [
    composer(:start),
    composer(:read_events, count: 2, acc: nil),
    composer(:check_ordering, acc: MapSet.new()),
    composer(:accumulate_waiting, acc: %{})
  ]

  def start({:start, paths}, nil, _) do
    events = Enum.map(paths, &{:start, &1})

    {events, nil}
  end

  def read_events({:start, path}, nil, _) do
    Process.sleep(1)
    {Enum.to_list(events_stream(path)), nil}
  end

  def check_ordering(event, order_numbers, _) do
    order_number = String.to_integer(event["order_number"])

    case event["type"] do
      "ORDER_CREATED" ->
        {[event], MapSet.put(order_numbers, order_number)}

      "PARCEL_SHIPPED" ->
        if MapSet.member?(order_numbers, order_number) do
          {[event], order_numbers}
        else
          {[Map.put(event, :wait, order_number)], order_numbers}
        end
    end
  end

  def accumulate_waiting(event, waiting, _) do
    case event["type"] do
      "ORDER_CREATED" ->
        order_number = String.to_integer(event["order_number"])
        {[event | Map.get(waiting, order_number, [])], Map.delete(waiting, order_number)}

      "PARCEL_SHIPPED" ->
        if event[:wait] do
          other_waiting = Map.get(waiting, event[:wait], [])
          {[], Map.put(waiting, event[:wait], [event | other_waiting])}
        else
          {[event], waiting}
        end
    end
  end

  defp events_stream(path) do
    Stream.resource(
      fn -> {File.open!(path), false} end,
      fn {file, headers} ->
        if String.ends_with?(path, "orders.csv") do
          Process.sleep(2)
        end

        case IO.read(file, :line) do
          line when is_binary(line) ->
            if headers do
              values = String.split(String.trim(line), ",")

              event =
                headers
                |> Enum.zip(values)
                |> Enum.into(%{})

              {[event], {file, headers}}
            else
              headers = String.split(String.trim(line), ",")
              {[], {file, headers}}
            end

          _ ->
            {:halt, {file, headers}}
        end
      end,
      fn {file, _headers} -> File.close(file) end
    )
  end
end

defmodule ALF.Examples.Parcels.OrderingTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.Ordering.Pipeline

  describe "with async pipeline" do
    setup do
      Pipeline.start()
      on_exit(&Pipeline.stop/0)
    end

    test "test with call" do
      {:start, ["test/examples/parcels/orders.csv", "test/examples/parcels/parcels.csv"]}
      |> Pipeline.call()
      |> check_order()
    end

    test "with stream" do
      [{:start, ["test/examples/parcels/orders.csv", "test/examples/parcels/parcels.csv"]}]
      |> Pipeline.stream()
      |> Enum.to_list()
      |> check_order()
    end
  end

  describe "with sync pipeline" do
    setup do
      Pipeline.start(sync: true)
      on_exit(&Pipeline.stop/0)
    end

    test "test with call" do
      {:start, ["test/examples/parcels/orders.csv", "test/examples/parcels/parcels.csv"]}
      |> Pipeline.call()
      |> check_order()
    end

    test "with stream" do
      [{:start, ["test/examples/parcels/orders.csv", "test/examples/parcels/parcels.csv"]}]
      |> Pipeline.stream()
      |> Enum.to_list()
      |> check_order()
    end
  end

  defp check_order(events) do
    order_111_index =
      Enum.find_index(events, &(&1["type"] == "ORDER_CREATED" && &1["order_number"] == "111"))

    parcel_111_index =
      Enum.find_index(events, &(&1["type"] == "PARCEL_SHIPPED" && &1["order_number"] == "111"))

    assert order_111_index < parcel_111_index

    order_222_index =
      Enum.find_index(events, &(&1["type"] == "ORDER_CREATED" && &1["order_number"] == "222"))

    parcel_222_index =
      Enum.find_index(events, &(&1["type"] == "PARCEL_SHIPPED" && &1["order_number"] == "222"))

    assert order_222_index < parcel_222_index

    order_333_index =
      Enum.find_index(events, &(&1["type"] == "ORDER_CREATED" && &1["order_number"] == "333"))

    parcel_333_index =
      Enum.find_index(events, &(&1["type"] == "PARCEL_SHIPPED" && &1["order_number"] == "333"))

    assert order_333_index < parcel_333_index
  end
end
