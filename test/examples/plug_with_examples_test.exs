defmodule ALF.Examples.PlugWith.HelloComponent do
  def call(%{name: name}, _) do
    "Hello #{name}!"
  end
end

defmodule ALF.Examples.PlugWith.Pipeline do
  use ALF.DSL

  alias ALF.Examples.PlugWith.HelloComponent

  defstruct [:input, :output]

  defmodule InputToName do
    alias ALF.Examples.PlugWith.Pipeline
    def plug(%Pipeline{input: input}, _), do: %{name: input}
    def unplug(string, prev_event, _), do: %{prev_event | output: string}
  end

  @components [
    stage(:build_struct),
    plug_with(InputToName, do: [stage(HelloComponent)]),
    stage(:format_output)
  ]

  def build_struct(event, _), do: %__MODULE__{input: event}
  def format_output(%__MODULE__{output: event}, _), do: event
end

defmodule ALF.Examples.PlugWithExamplesTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.PlugWith.Pipeline

  setup do
    Pipeline.start()
    Process.sleep(10)
    on_exit(&Pipeline.stop/0)
  end

  test "process input" do
    inputs = ["Anton", "Baton"]

    results =
      inputs
      |> Pipeline.stream()
      |> Enum.to_list()

    assert Enum.sort(results) == ["Hello Anton!", "Hello Baton!"]
  end
end
