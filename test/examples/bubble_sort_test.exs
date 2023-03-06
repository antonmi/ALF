defmodule ALF.Examples.BubbleSort.Pipeline do
  use ALF.DSL

  defstruct [:list, :new_list, :max, :ready]

  @components [
    stage(:build_struct),
    goto_point(:goto_point),
    stage(:find_max),
    stage(:update_new_list, count: 10),
    stage(:rebuild_list, count: 10),
    clone(:logging, to: [stage(:report_step), dead_end(:after_report)]),
    goto(:goto_if, to: :goto_point),
    stage(:format_output)
  ]

  def build_struct(list, _) do
    %__MODULE__{list: list, new_list: [], max: 0, ready: false}
  end

  def find_max(struct, _) do
    %{struct | max: Enum.max(struct.list)}
  end

  def update_new_list(struct, _) do
    %{struct | new_list: [struct.max | struct.new_list]}
  end

  def rebuild_list(struct, _) do
    %{struct | list: struct.list -- [struct.max]}
  end

  def goto_if(struct, _) do
    !Enum.empty?(struct.list)
  end

  def report_step(struct, _) do
    # IO.inspect("Step: #{inspect struct}", charlists: :as_lists)
    struct
  end

  def format_output(struct, _) do
    struct.new_list
  end
end

defmodule ALF.Examples.BubbleSortTest do
  use ExUnit.Case

  alias ALF.Examples.BubbleSort.Pipeline

  @range 1..5

  setup do
    Pipeline.start()
  end

  test "sort a list" do
    result =
      1..5
      |> Enum.shuffle()
      |> Pipeline.call()

    assert result == [1, 2, 3, 4, 5]

    results =
      [Enum.shuffle(1..5), Enum.shuffle(1..5)]
      |> Pipeline.stream()
      |> Enum.to_list()

    assert results == [[1, 2, 3, 4, 5], [1, 2, 3, 4, 5]]
  end

  test "sort many lists" do
    list_of_lists = Enum.map(1..5, fn _i -> Enum.shuffle(@range) end)

    results =
      list_of_lists
      |> Enum.map(&Pipeline.stream([&1]))
      |> Enum.map(&Task.async(fn -> hd(Enum.to_list(&1)) end))
      |> Task.await_many()

    Enum.each(results, fn result ->
      assert result == Enum.to_list(@range)
    end)
  end
end
