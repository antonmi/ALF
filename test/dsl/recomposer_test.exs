defmodule ALF.DSL.RecomposerTest do
  use ExUnit.Case, async: true

  alias ALF.Builder

  alias ALF.Components.{
    Recomposer
  }

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  describe "Recomposer with function as a name" do
    defmodule PipelineRecomposer1 do
      use ALF.DSL

      @components [
        recomposer(:the_recomposer, opts: [foo: :bar])
      ]
    end

    test "build PipelineRecomposer1", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineRecomposer1, sup_pid, Helpers.random_atom("manager"))

      [recomposer] = pipeline.components

      assert %Recomposer{
               name: :the_recomposer,
               module: PipelineRecomposer1,
               function: :the_recomposer,
               opts: [foo: :bar],
               pipe_module: PipelineRecomposer1,
               pipeline_module: PipelineRecomposer1
             } = recomposer
    end
  end

  describe "Recomposer with module as a name" do
    defmodule PipelineRecomposer2 do
      use ALF.DSL

      defmodule RecomposerModule do
        def init(opts), do: Keyword.put(opts, :baz, :qux)

        def call(event, _), do: [event]
      end

      @components [
        recomposer(RecomposerModule, opts: [foo: :bar])
      ]
    end

    test "build PipelineRecomposer2", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineRecomposer2, sup_pid, Helpers.random_atom("manager"))

      [recomposer] = pipeline.components
      recomposer = Recomposer.__state__(recomposer.pid)

      assert %Recomposer{
               name: PipelineRecomposer2.RecomposerModule,
               module: PipelineRecomposer2.RecomposerModule,
               function: :call,
               opts: [baz: :qux, foo: :bar],
               pipe_module: PipelineRecomposer2,
               pipeline_module: PipelineRecomposer2
             } = recomposer
    end
  end

  describe "Recomposer with custom name" do
    defmodule PipelineRecomposer3 do
      use ALF.DSL

      @components [
        recomposer(:the_recomposer, opts: [foo: :bar], name: :custom_name)
      ]
    end

    test "build PipelineRecomposer3", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineRecomposer3, sup_pid, Helpers.random_atom("manager"))

      [recomposer] = pipeline.components

      assert %Recomposer{
               name: :custom_name,
               module: PipelineRecomposer3,
               function: :the_recomposer,
               opts: [foo: :bar],
               pipe_module: PipelineRecomposer3,
               pipeline_module: PipelineRecomposer3
             } = recomposer
    end
  end
end
