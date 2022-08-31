defmodule ALF.DSL.DecomposerTest do
  use ExUnit.Case, async: true

  alias ALF.Builder

  alias ALF.Components.{
    Decomposer
  }

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  describe "Decomposer with function as a name" do
    defmodule PipelineDecomposer1 do
      use ALF.DSL

      @components [
        decomposer(:the_decomposer, opts: [foo: :bar])
      ]
    end

    test "build PipelineDecomposer1", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineDecomposer1, sup_pid, :manager)

      [decomposer] = pipeline.components

      assert %Decomposer{
               name: :the_decomposer,
               module: PipelineDecomposer1,
               function: :the_decomposer,
               opts: [foo: :bar],
               pipe_module: PipelineDecomposer1,
               pipeline_module: PipelineDecomposer1
             } = decomposer
    end
  end

  describe "Decomposer with module as a name" do
    defmodule PipelineDecomposer2 do
      use ALF.DSL

      defmodule DecomposerModule do
        def init(opts), do: Keyword.put(opts, :baz, :qux)

        def call(event, _), do: [event]
      end

      @components [
        decomposer(DecomposerModule, opts: [foo: :bar])
      ]
    end

    test "build PipelineDecomposer2", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineDecomposer2, sup_pid, :manager)

      [decomposer] = pipeline.components
      decomposer = Decomposer.__state__(decomposer.pid)

      assert %Decomposer{
               name: PipelineDecomposer2.DecomposerModule,
               module: PipelineDecomposer2.DecomposerModule,
               function: :call,
               opts: [baz: :qux, foo: :bar],
               pipe_module: PipelineDecomposer2,
               pipeline_module: PipelineDecomposer2
             } = decomposer
    end
  end

  describe "Decomposer with custom name" do
    defmodule PipelineDecomposer3 do
      use ALF.DSL

      @components [
        decomposer(:the_decomposer, opts: [foo: :bar], name: :custom_name)
      ]
    end

    test "build PipelineDecomposer3", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineDecomposer3, sup_pid, :manager)

      [decomposer] = pipeline.components

      assert %Decomposer{
               name: :custom_name,
               module: PipelineDecomposer3,
               function: :the_decomposer,
               opts: [foo: :bar],
               pipe_module: PipelineDecomposer3,
               pipeline_module: PipelineDecomposer3
             } = decomposer
    end
  end
end
