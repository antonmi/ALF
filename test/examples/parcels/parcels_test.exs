defmodule ALF.Examples.Parcels.Pipeline do
  use ALF.DSL

  @components [
    composer(:check_expired, acc: []),
    composer(:check_parcels_count, acc: %{})
  ]

  @seconds_in_week 3600 * 24 * 7

  def check_expired(event, acc, _) do
    order_number = event["order_number"]

    case event["type"] do
      "ORDER_CREATED" ->
        {:ok, time, _} = DateTime.from_iso8601(event["occurred_at"])
        acc = [{order_number, time} | acc]
        {[event], acc}

      "PARCEL_SHIPPED" ->
        {:ok, parcel_time, _} = DateTime.from_iso8601(event["occurred_at"])

        {expired, still_valid} =
          Enum.split_while(Enum.reverse(acc), fn {_, order_time} ->
            DateTime.diff(parcel_time, order_time, :second) > @seconds_in_week
          end)

        expired_events =
          Enum.map(expired, fn {order_number, _time} ->
            %{"type" => "THRESHOLD_EXCEEDED", "order_number" => order_number}
          end)

        {expired_events ++ [event], still_valid}
    end
  end

  def check_parcels_count(event, acc, _) do
    order_number = event["order_number"]

    case event["type"] do
      "ORDER_CREATED" ->
        amount = String.to_integer(event["parcels_to_ship"])
        acc = Map.put(acc, order_number, amount)
        {[], acc}

      "PARCEL_SHIPPED" ->
        case Map.get(acc, order_number) do
          nil ->
            {[], acc}

          1 ->
            ok_event = %{
              "type" => "ALL_PARCELS_SHIPPED",
              "order_number" => order_number,
              "occurred_at" => event["occurred_at"]
            }

            acc = Map.put(acc, order_number, :all_parcels_shipped)
            {[ok_event], acc}

          amount when amount > 1 ->
            acc = Map.put(acc, order_number, amount - 1)
            {[], acc}
        end

      "THRESHOLD_EXCEEDED" ->
        case Map.get(acc, order_number) do
          :all_parcels_shipped ->
            {[], Map.delete(acc, order_number)}

          _count ->
            {[event], Map.delete(acc, order_number)}
        end
    end
  end
end

defmodule ALF.Examples.Parcels.ParcelsTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.Parcels.Pipeline

  setup do
    Pipeline.start()
    on_exit(&Pipeline.stop/0)
  end

  def events_stream do
    Stream.resource(
      fn -> {File.open!("test/examples/parcels/events.csv"), false} end,
      fn {file, headers} ->
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

  test "test" do
    results =
      events_stream()
      |> Pipeline.stream()
      |> Enum.to_list()

    assert results == [
             %{
               "order_number" => "111",
               "type" => "ALL_PARCELS_SHIPPED",
               "occurred_at" => "2017-04-21T08:00:00.000Z"
             },
             %{"order_number" => "222", "type" => "THRESHOLD_EXCEEDED"},
             %{"order_number" => "333", "type" => "THRESHOLD_EXCEEDED"}
           ]
  end
end
