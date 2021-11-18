defmodule ALF.Examples.BubbleSortWithSwitch.Pipeline do
  use ALF.DSL

  defstruct [:list, :new_list, :max, :ready]

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

  def build_struct(list, _opts) do
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

  def report_step(struct, _opts) do
    #    IO.inspect("Step: #{inspect struct}", charlists: :as_lists)
    struct
  end

  def format_output(struct, _opts) do
    struct.new_list
  end

  def ready_or_not(struct, _opts) do
    if Enum.empty?(struct.list) do
      :ready
    else
      :not_ready
    end
  end
end


defmodule ALF.ExamplesBubbleSortWithSwitchTest do
  use ExUnit.Case

  alias ALF.Examples.BubbleSortWithSwitch.Pipeline
  alias ALF.Manager

  @range 1..5

  setup do: Manager.start(Pipeline)

  test "sort" do
    [result] =
      [Enum.shuffle(@range)]
      |> Manager.stream_to(Pipeline)
      |> Enum.to_list()

    assert result == Enum.to_list(@range)
  end
end
