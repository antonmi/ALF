defmodule ALF.Examples.AddMultMinus.Pipeline do
  use ALF.DSL

  @components [
    stage(:add_one),
    stage(:mult_by_two),
    stage(:minus_three)
  ]

  def add_one(datum, _opts), do: datum + 1
  def mult_by_two(datum, _opts), do: datum * 2
  def minus_three(datum, _opts), do: datum - 3
end

defmodule ALF.Examples.AddMultMinusTest do
  use ExUnit.Case

  alias ALF.Examples.AddMultMinus.Pipeline
  alias ALF.Manager

  setup do: Manager.start(Pipeline)

  test "several inputs" do
    results =
      [1, 2, 3]
      |> Manager.stream_to(Pipeline)
      |> Enum.to_list()

    assert results == [1, 3, 5]
  end
end
