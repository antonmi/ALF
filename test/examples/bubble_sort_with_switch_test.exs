defmodule ALF.Examples.BubbleSortWithSwitch.Pipeline do
  use ALF.DSL

  defstruct [:list, :new_list, :max, :ready]

  @components [
    stage(:build_struct),
    goto_point(:goto_point),
    stage(:find_max),
    stage(:update_new_list),
    stage(:rebuild_list),
    switch(:ready_or_not,
      branches: %{
        ready: [stage(:format_output)],
        not_ready: [goto(true, to: :goto_point, name: :just_go)]
      }
    )
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

  def format_output(struct, _) do
    struct.new_list
  end

  def ready_or_not(struct, _) do
    if Enum.empty?(struct.list) do
      :ready
    else
      :not_ready
    end
  end
end

defmodule ALF.ExamplesBubbleSortWithSwitchTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.BubbleSortWithSwitch.Pipeline

  @range 1..5

  setup do
    Pipeline.start()
    Process.sleep(10)
    on_exit(&Pipeline.stop/0)
  end

  test "sort" do
    [result] =
      [Enum.shuffle(@range)]
      |> Pipeline.stream()
      |> Enum.to_list()

    assert result == Enum.to_list(@range)
  end
end

defmodule ALF.ExamplesBubbleSortWithSwitchAndPlugTest do
  use ExUnit.Case, async: true

  defmodule PluggedPipeline do
    use ALF.DSL

    alias ALF.Examples.BubbleSortWithSwitch.Pipeline

    defmodule MyPlug do
      def plug(event, _), do: event
      def unplug(event, _prev_event, _), do: event
    end

    @components [
      plug_with(MyPlug, do: [from(Pipeline)])
    ]
  end

  @range 1..5

  setup do
    PluggedPipeline.start()
    Process.sleep(10)
    on_exit(&PluggedPipeline.stop/0)
  end

  test "sort" do
    [result] =
      [Enum.shuffle(@range)]
      |> PluggedPipeline.stream()
      |> Enum.to_list()

    assert result == Enum.to_list(@range)
  end
end
