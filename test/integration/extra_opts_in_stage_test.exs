defmodule ALF.PipelineModuleAccessInStageTest do
  use ExUnit.Case, async: true

  alias ALF.Manager

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(:add, extra_opts: true),
      stage(:mult, extra_opts: true)
    ]

    def add(datum, opts), do: datum + opts[:pipe_module].to_add()

    def mult(datum, opts), do: datum * opts[:pipeline_module].to_mult()

    def to_add, do: 1
    def to_mult, do: 2
  end

  describe "simple case" do
    setup do
      Manager.start(SimplePipeline)
    end

    test "run stream" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(SimplePipeline)
        |> Enum.to_list()

      assert results == [4, 6, 8]
    end
  end

  describe "when pipeline is reused" do
    defmodule CompositePipeline do
      use ALF.DSL

      @components stages_from(SimplePipeline, extra_opts: true)

      def to_mult, do: 3
    end

    setup do
      Manager.start(CompositePipeline)
    end

    test "it gets to_mult from the pipeline" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(CompositePipeline)
        |> Enum.to_list()

      assert results == [6, 9, 12]
    end
  end
end
