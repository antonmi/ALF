defmodule ALF.DSL.GotoTest do
  use ExUnit.Case, async: true

  alias ALF.Builder

  alias ALF.Components.{
    GotoPoint,
    Goto
  }

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  describe "Goto with function as a name" do
    defmodule PipelineGoto1 do
      use ALF.DSL

      @components [
        goto_point(:goto_point),
        goto(:goto, to: :goto_point, opts: [foo: :bar])
      ]
    end

    test "build PipelineGoto1", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineGoto1, sup_pid, false)

      [point, goto] = pipeline.components

      assert %GotoPoint{
               name: :goto_point,
               pid: point_pid,
               pipe_module: PipelineGoto1,
               pipeline_module: PipelineGoto1
             } = point

      assert is_pid(point_pid)

      assert %Goto{
               name: :goto,
               module: PipelineGoto1,
               function: :goto,
               to: :goto_point,
               opts: [foo: :bar],
               pipe_module: PipelineGoto1,
               pipeline_module: PipelineGoto1
             } = goto
    end
  end

  describe "Goto with module as a name" do
    defmodule PipelineGoto2 do
      use ALF.DSL

      defmodule GotoModule do
        def init(opts), do: Keyword.put(opts, :baz, :qux)

        def call(_event, _), do: true
      end

      @components [
        goto_point(:goto_point),
        goto(GotoModule, to: :goto_point, opts: [foo: :bar])
      ]
    end

    test "build PipelineGoto2", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineGoto2, sup_pid, false)

      [_point, goto] = pipeline.components
      goto = Goto.__state__(goto.pid)

      assert %Goto{
               name: PipelineGoto2.GotoModule,
               module: PipelineGoto2.GotoModule,
               function: :call,
               to: :goto_point,
               opts: [baz: :qux, foo: :bar],
               pipe_module: PipelineGoto2,
               pipeline_module: PipelineGoto2
             } = goto
    end
  end

  describe "Goto with custom name" do
    defmodule PipelineGoto3 do
      use ALF.DSL

      @components [
        goto_point(:goto_point),
        goto(:goto, to: :goto_point, opts: [foo: :bar], name: :custom_name)
      ]
    end

    test "build PipelineGoto1", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineGoto3, sup_pid, false)

      [_point, goto] = pipeline.components

      assert %Goto{
               name: :custom_name,
               module: PipelineGoto3,
               function: :goto,
               to: :goto_point,
               opts: [foo: :bar],
               pipe_module: PipelineGoto3,
               pipeline_module: PipelineGoto3
             } = goto
    end
  end
end
