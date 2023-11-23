defmodule ALF.Examples.StreamFile.Pipeline do
  use ALF.DSL

  @components [
    goto_point(:request_new_line),
    composer(:stream_file, acc: {nil, []}),
    goto(:new_line, to: :request_new_line)
    #    stage(:inspect)
  ]

  def stream_file(path, {nil, []}, _) do
    file = File.open!(path)

    case IO.read(file, :line) do
      line when is_binary(line) ->
        headers = String.split(String.trim(line), ",")
        {[:next_line_event], {file, headers}}

      _ ->
        File.close(file)
        {[], {nil, []}}
    end
  end

  def stream_file(:next_line_event, {file, headers}, _) do
    case IO.read(file, :line) do
      line when is_binary(line) ->
        values = String.split(String.trim(line), ",")

        event =
          headers
          |> Enum.zip(values)
          |> Enum.into(%{})

        {[event, :next_line_event], {file, headers}}

      _ ->
        File.close(file)
        {[], {nil, []}}
    end
  end

  def new_line(:next_line_event, _), do: true
  def new_line(_event, _), do: false

  def inspect(event, _), do: IO.inspect(event)
end

defmodule ALF.Examples.Parcels.StreamFileTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.StreamFile.Pipeline

  describe "with async pipeline" do
    setup do
      Pipeline.start()
      on_exit(&Pipeline.stop/0)
    end

    test "test with call" do
      assert length(Pipeline.call("test/examples/parcels/orders.csv")) == 3
    end

    test "with stream" do
      all_events =
        ["test/examples/parcels/orders.csv", "test/examples/parcels/parcels.csv"]
        |> Pipeline.stream()
        |> Enum.to_list()

      assert length(all_events) == 9
    end

    #        test "huge file" do
    #          assert length(Pipeline.call("test/examples/parcels/parcels_huge.csv")) == 3
    #          ["test/examples/parcels/parcels_huge.csv"]
    #          |> Pipeline.stream()
    #          |> Enum.to_list()
    #        end
  end

  describe "with sync pipeline" do
    setup do
      Pipeline.start(sync: true)
      on_exit(&Pipeline.stop/0)
    end

    test "test with call" do
      assert length(Pipeline.call("test/examples/parcels/orders.csv")) == 3
    end

    test "with stream" do
      all_events =
        ["test/examples/parcels/orders.csv", "test/examples/parcels/parcels.csv"]
        |> Pipeline.stream()
        |> Enum.to_list()

      assert length(all_events) == 9
    end
  end
end
