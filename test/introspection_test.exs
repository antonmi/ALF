defmodule ALF.IntrospectionTest do
  use ExUnit.Case, async: false

  alias ALF.{Introspection, Manager}

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(:add_one),
      stage(:mult_two)
    ]

    def add_one(event, _), do: event + 1
    def mult_two(event, _), do: event * 2
  end

  setup do
    Introspection.reset()
  end

  describe "pipelines" do
    setup do
      Manager.start(SimplePipeline)
    end

    test "it returns pipelines list" do
      set = Introspection.pipelines()
      assert MapSet.member?(set, SimplePipeline)
    end

    test "it returns pipeline info" do
      list =
        Introspection.info(SimplePipeline)
        |> IO.inspect()

      assert length(list) > 0
    end

    test "when pipeline is stopped" do
      Manager.stop(SimplePipeline)
      set = Introspection.pipelines()
      assert MapSet.size(set) == 0
    end
  end
end
