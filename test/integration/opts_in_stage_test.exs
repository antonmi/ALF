defmodule ALF.OptsInStageTest do
  use ExUnit.Case, async: false

  alias ALF.Manager

  defmodule ComponentA do
    def init(opts) do
      Map.put(opts, :b, opts[:a])
    end

    def call(datum, opts) do
      datum + opts[:a] + opts[:b]
    end
  end

  defmodule ComponentB do
    def call(datum, opts) do
      datum + opts[:c]
    end
  end

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(ComponentA, opts: %{a: 1}),
      stage(ComponentB, opts: [c: 10]),
      stage(:inline, opts: [b: 2])
    ]

    def inline(datum, opts) do
      datum + opts[:b]
    end
  end

  describe "opts" do
    setup do
      Manager.start(SimplePipeline)
    end

    test "run stream" do
      [result] =
        [1]
        |> Manager.stream_to(SimplePipeline)
        |> Enum.to_list()

      assert result == 15
    end
  end

  describe "overrides in stages_from" do
    defmodule PipelineA do
      use ALF.DSL

      @components [
        stage(:foo, opts: %{foo: :bar, module: __MODULE__})
      ]

      def foo(datum, opts) do
        "#{datum}-#{opts[:foo]}-#{opts[:module]}-#{opts[:aaa]}"
      end
    end

    defmodule PipelineB do
      use ALF.DSL

      @components stages_from(PipelineA, opts: [module: __MODULE__, aaa: :bbb]) ++
                    [stage(:bar, opts: %{module: __MODULE__})]

      def bar(datum, _opts) do
        datum
      end
    end

    setup do
      Manager.start(PipelineA)
      Manager.start(PipelineB)
    end

    test "check PipelineA module" do
      [result] =
        ["hey"]
        |> Manager.stream_to(PipelineA)
        |> Enum.to_list()

      assert result == "hey-bar-Elixir.ALF.OptsInStageTest.PipelineA-"
    end

    test "check PipelineB module" do
      [result] =
        ["hey"]
        |> Manager.stream_to(PipelineB)
        |> Enum.to_list()

      assert result == "hey-bar-Elixir.ALF.OptsInStageTest.PipelineB-bbb"
    end
  end
end
