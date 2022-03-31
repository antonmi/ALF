defmodule ALF.IntrospectionTest do
  use ExUnit.Case, async: false

  alias ALF.{Introspection, Manager}

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(:add_one, count: 2),
      stage(:mult_two)
    ]

    def add_one(event, _), do: event + 1
    def mult_two(event, _), do: event * 2
  end

  defmodule AnotherPipeline do
    use ALF.DSL

    @components [
      stage(:add_one),
      stage(:mult_two)
    ]

    def minus_one(event, _), do: event - 1
    def mult_three(event, _), do: event * 3
  end

  setup_all do
    Introspection.reset()
    :ok
  end

  describe "pipelines and info" do
    setup do
      Manager.start(SimplePipeline)
      Manager.start(AnotherPipeline)
      :ok
    end

    test "it returns pipelines list" do
      set = Introspection.pipelines()
      assert MapSet.member?(set, SimplePipeline)
      assert MapSet.member?(set, AnotherPipeline)
    end

    test "it returns pipeline info" do
      list = Introspection.info(SimplePipeline)

      names = Enum.map(list, & &1[:name])
      assert names == [:producer, :add_one, :add_one, :mult_two, :consumer]

      [_, add_one1, add_one2, _, _] = list
      assert %{count: 2, name: :add_one, number: 0, opts: []} = add_one1
      assert %{count: 2, name: :add_one, number: 1, opts: []} = add_one2
    end

    test "when pipeline is stopped" do
      Manager.stop(SimplePipeline)
      Manager.stop(AnotherPipeline)
      set = Introspection.pipelines()
      assert MapSet.size(set) == 0
    end
  end
end
