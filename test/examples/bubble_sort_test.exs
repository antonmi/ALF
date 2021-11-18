defmodule ALF.Examples.BubbleSort.Pipeline do
  use ALF.DSL

  defstruct [:list, :new_list, :max, :ready]

  @components [
    stage(:build_struct),
    goto_point(:go_to_point),
    stage(:find_max),
    stage(:update_new_list, count: 10),
    stage(:rebuild_list, count: 10),
    clone(:logging, to: [stage(:report_step), dead_end(:after_report)]),
    goto(:the_loop, to: :go_to_point, if: :go_to_condition),
    stage(:format_output)
  ]

  def build_struct(list, _pts) do
    %__MODULE__{list: list, new_list: [], max: 0, ready: false}
  end

  def find_max(struct, _opts) do
    %{struct | max: Enum.max(struct.list)}
  end

  def update_new_list(struct, _opts) do
    %{struct | new_list: [struct.max | struct.new_list]}
  end

  def rebuild_list(struct, _opts) do
    %{struct | list: struct.list -- [struct.max]}
  end

  def go_to_condition(struct, _opts) do
    !Enum.empty?(struct.list)
  end

  def report_step(struct, _opts) do
    #    IO.inspect("Step: #{inspect struct}", charlists: :as_lists)
    struct
  end

  def format_output(struct, _opts) do
    struct.new_list
  end
end

defmodule ALF.Examples.BubbleSort.Pipeline2 do
  use ALF.DSL

  defstruct [:list, :new_list, :max, :ready]

  alias ALF.Examples.BubbleSort.Pipeline

  @components [
    stage(:build_struct),
    goto_point(:go_to_point),
    stage(:find_max),
    stage(:update_new_list, count: 10),
    stage(:rebuild_list, count: 10),
    clone(:logging, to: [stage(:report_step), dead_end(:after_report)]),
    switch(:ready_or_not,
      branches: %{
        ready: [stage(:format_output)],
        not_ready: [goto(:the_loop, to: :go_to_point, if: true)]
      },
      cond: :ready_or_not
    )
  ]

  defdelegate build_struct(datum, opts), to: Pipeline
  defdelegate find_max(struct, opts), to: Pipeline
  defdelegate update_new_list(struct, opts), to: Pipeline
  defdelegate rebuild_list(struct, opts), to: Pipeline
  defdelegate format_output(struct, opts), to: Pipeline

  def ready_or_not(struct, _opts) do
    if Enum.empty?(struct.list) do
      :ready
    else
      :not_ready
    end
  end
end

defmodule ALF.Examples.BubbleSortTest do
  use ExUnit.Case, async: false

  alias ALF.Examples.BubbleSort.Pipeline
  alias ALF.Manager

  @range 1..5

  setup do: Manager.start(Pipeline)

  test "sort many lists" do
    list_of_lists = Enum.map(1..5, fn _i -> Enum.shuffle(@range) end)

    results =
      list_of_lists
      |> Enum.map(&Manager.stream_to([&1], Pipeline))
      |> Enum.map(&Task.async(fn -> hd(Enum.to_list(&1)) end))
      |> Task.await_many(:infinity)

    Enum.each(results, fn result ->
      assert result == Enum.to_list(@range)
    end)
  end
end

defmodule ALF.Examples.BubbleSort2Test do
  use ExUnit.Case, async: false

  alias ALF.Examples.BubbleSort.Pipeline2
  alias ALF.Manager

  @range 1..5

  setup do: Manager.start(Pipeline2)

  test "sort" do
    [result] =
      [Enum.shuffle(@range)]
      |> Manager.stream_to(Pipeline2)
      |> Enum.to_list()

    assert result == Enum.to_list(@range)
  end
end
