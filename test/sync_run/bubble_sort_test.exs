defmodule ALF.SyncRun.BubbleSort.Pipeline do
  use ALF.DSL

  defstruct [:list, :new_list, :max, :ready]

  @components [
    stage(:build_struct),
    goto_point(:goto_point),
    stage(:find_max),
    stage(:update_new_list, count: 10),
    stage(:rebuild_list, count: 10),
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

  def format_output(struct, _) do
    struct.new_list
  end
end

defmodule ALF.SyncRun.BubbleSortTest do
  use ExUnit.Case, async: true

  alias ALF.SyncRun.BubbleSort.Pipeline

  @range 1..5

  setup do
    Pipeline.start(sync: true)
    on_exit(&Pipeline.stop/0)
  end

  test "sort one list" do
    list = Enum.shuffle(@range)

    results =
      [list]
      |> Pipeline.stream()
      |> Enum.to_list()

    assert results == [Enum.to_list(@range)]
  end

  test "sort many lists" do
    list_of_lists = Enum.map(1..5, fn _i -> Enum.shuffle(@range) end)

    results =
      list_of_lists
      |> Enum.map(&Pipeline.stream([&1]))
      |> Enum.map(&Task.async(fn -> hd(Enum.to_list(&1)) end))
      |> Task.await_many(:infinity)

    Enum.each(results, fn result ->
      assert result == Enum.to_list(@range)
    end)
  end
end
