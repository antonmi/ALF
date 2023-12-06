defmodule ALF.Integration.FlowInterfaceTest do
  use ExUnit.Case, async: true

  defmodule Pipeline do
    use ALF.DSL

    @components [
      stage(:add_one),
      stage(:mult_by_two),
      stage(:minus_three)
    ]

    def add_one(event, _), do: event + 1
    def mult_by_two(event, _), do: event * 2
    def minus_three(event, _), do: event - 3
  end

  setup do
    Pipeline.start()
    on_exit(&Pipeline.stop/0)
  end

  test "flow with list of names" do
    flow = %{s1: [1, 2, 3], s2: [4, 5, 6], s3: [7, 8, 9]}

    flow = Pipeline.flow(flow, [:s1, :s2])

    assert Enum.sort(Enum.to_list(flow[:s1])) == [1, 3, 5]
    assert Enum.sort(Enum.to_list(flow[:s2])) == [7, 9, 11]
    assert Enum.sort(Enum.to_list(flow[:s3])) == [7, 8, 9]
  end

  test "flow with one name" do
    flow = %{s1: [1, 2, 3], s2: [4, 5, 6]}
    flow = Pipeline.flow(flow, :s1)
    assert Enum.sort(Enum.to_list(flow[:s1])) == [1, 3, 5]
    assert Enum.sort(Enum.to_list(flow[:s2])) == [4, 5, 6]
  end
end
