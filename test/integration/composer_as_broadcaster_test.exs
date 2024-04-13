defmodule ALF.Examples.ComposerAsBroadcasterTest do
  use ExUnit.Case, async: true

  defmodule Pipeline do
    use ALF.DSL

    @components [
      composer(:triplicate),
      switch(:route,
        branches: %{
          first: [stage(:get_number), stage(:add_one)],
          second: [stage(:get_number), stage(:mult_two)],
          third: [stage(:get_number), stage(:minus_three)]
        }
      )
    ]

    def triplicate(event, _memo, _) do
      {[{:first, event}, {:second, event}, {:third, event}], nil}
    end

    def route(event, _), do: elem(event, 0)

    def get_number(event, _), do: elem(event, 1)

    def add_one(event, _), do: event + 1
    def mult_two(event, _), do: event * 2
    def minus_three(event, _), do: event - 3
  end

  setup do
    Pipeline.start(sync: true)
    on_exit(&Pipeline.stop/0)
  end

  test "one stream" do
    results = Pipeline.call(2)
    assert results == [3, 4, -1]
  end
end
