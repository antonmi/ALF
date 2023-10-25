defmodule ALF.DSL.SwitchTest do
  use ExUnit.Case, async: true

  alias ALF.Builder

  alias ALF.Components.{
    Switch,
    Stage
  }

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  describe "Switch with function as a name" do
    defmodule PipelineSwitch1 do
      use ALF.DSL

      @components [
        switch(:do_switch,
          branches: %{
            part1: [stage(:a)],
            part2: [stage(:b)]
          },
          opts: [foo: :bar]
        )
      ]
    end

    test "build PipelineSwitch1", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineSwitch1, sup_pid, false)

      [switch] = pipeline.components

      assert %Switch{
               name: :do_switch,
               module: PipelineSwitch1,
               function: :do_switch,
               branches: %{
                 part1: [stage_a],
                 part2: [stage_b]
               },
               opts: [foo: :bar],
               pid: pid,
               pipeline_module: PipelineSwitch1
             } = switch

      assert is_pid(pid)
      assert %Stage{name: :a} = stage_a
      assert %Stage{name: :b} = stage_b
    end
  end

  describe "Switch with Module as a name" do
    defmodule PipelineSwitch2 do
      use ALF.DSL

      defmodule DoSwitch do
        def init(opts), do: Keyword.put(opts, :baz, :qux)

        def call(_event, _), do: :part1
      end

      @components [
        switch(DoSwitch,
          branches: %{
            part1: [stage(:a)],
            part2: [stage(:b)]
          },
          opts: [foo: :bar]
        )
      ]
    end

    test "build PipelineSwitch2", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineSwitch2, sup_pid, false)

      [switch] = pipeline.components
      switch = Switch.__state__(switch.pid)

      assert %Switch{
               name: PipelineSwitch2.DoSwitch,
               module: PipelineSwitch2.DoSwitch,
               function: :call,
               opts: [baz: :qux, foo: :bar],
               pipeline_module: PipelineSwitch2
             } = switch
    end
  end

  describe "Switch with custom name" do
    defmodule PipelineSwitch3 do
      use ALF.DSL

      @components [
        switch(:do_switch,
          branches: %{
            part1: [stage(:a)],
            part2: [stage(:b)]
          },
          opts: [foo: :bar],
          name: :custom_name
        )
      ]
    end

    test "build PipelineSwitch2", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineSwitch3, sup_pid, false)

      [switch] = pipeline.components

      assert %Switch{
               name: :custom_name,
               module: PipelineSwitch3,
               function: :do_switch,
               opts: [foo: :bar],
               pipeline_module: PipelineSwitch3
             } = switch
    end
  end

  describe "Switch without branches" do
    test "build PipelineSwitch2", %{sup_pid: sup_pid} do
      assert_raise ALF.DSLError,
                   "There must be at least one branch in the do_switch switch.",
                   fn ->
                     defmodule PipelineSwitch4 do
                       use ALF.DSL

                       @components [
                         switch(:do_switch,
                           branches: %{},
                           opts: [foo: :bar],
                           name: :custom_name
                         )
                       ]
                     end
                   end
    end
  end
end
