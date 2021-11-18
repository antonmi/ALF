defmodule ALF.Examples.PlugWith.HelloComponent do
  def call(%{name: name}, _opts) do
    "Hello #{name}!"
  end
end

defmodule ALF.Examples.PlugWith.Pipeline do
  use ALF.DSL

  alias ALF.Examples.PlugWith.HelloComponent

  defstruct [:input, :output]

  defmodule InputToName do
    alias ALF.Examples.PlugWith.Pipeline
    def plug(%Pipeline{input: input}, _opts), do: %{name: input}
    def unplug(string, prev_datum, _opts), do: %{prev_datum | output: string}
  end

  @components [
    stage(:build_struct),
    plug_with(InputToName) do
      [stage(HelloComponent)]
    end,
    stage(:format_output)
  ]

  def build_struct(datum, _opts), do: %__MODULE__{input: datum}
  def format_output(%__MODULE__{output: datum}, _opts), do: datum
end

defmodule ALF.Examples.PlugWithExamplesTest do
  use ExUnit.Case

  alias ALF.Examples.PlugWith.Pipeline
  alias ALF.Manager

  setup do: Manager.start(Pipeline)

  test "sort many lists" do
    inputs = ["Anton", "Baton"]

    results =
      inputs
      |> Manager.stream_to(Pipeline)
      |> Enum.to_list()

    assert results == ["Hello Anton!", "Hello Baton!"]
  end
end
