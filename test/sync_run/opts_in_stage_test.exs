defmodule ALF.SyncRun.OptsInStageTest do
  use ExUnit.Case, async: false

  defmodule ComponentA do
    def init(opts) do
      Map.put(opts, :b, opts[:a])
    end

    def call(event, opts) do
      event + opts[:a] + opts[:b]
    end
  end

  defmodule ComponentB do
    def call(event, opts) do
      event + opts[:c]
    end
  end

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(ComponentA, opts: %{a: 1}),
      stage(ComponentB, opts: [c: 10]),
      stage(:inline, opts: [b: 2])
    ]

    def inline(event, opts) do
      event + opts[:b]
    end
  end

  describe "opts" do
    setup do
      SimplePipeline.start(sync: true)
      on_exit(&SimplePipeline.stop/0)
    end

    test "run stream" do
      [result] =
        [1]
        |> SimplePipeline.stream()
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

      def foo(event, opts) do
        "#{event}-#{opts[:foo]}-#{opts[:module]}-#{opts[:aaa]}"
      end
    end

    defmodule PipelineB do
      use ALF.DSL

      @components stages_from(PipelineA, opts: [module: __MODULE__, aaa: :bbb]) ++
                    [stage(:bar, opts: %{module: __MODULE__})]

      def bar(event, _) do
        event
      end
    end

    setup do
      PipelineA.start(sync: true)
      PipelineB.start(sync: true)

      on_exit(fn ->
        PipelineA.stop()
        PipelineB.stop()
      end)
    end

    test "check PipelineA module" do
      [result] =
        ["hey"]
        |> PipelineA.stream()
        |> Enum.to_list()

      assert result == "hey-bar-Elixir.ALF.SyncRun.OptsInStageTest.PipelineA-"
    end

    test "check PipelineB module" do
      [result] =
        ["hey"]
        |> PipelineB.stream()
        |> Enum.to_list()

      assert result == "hey-bar-Elixir.ALF.SyncRun.OptsInStageTest.PipelineB-bbb"
    end
  end
end
