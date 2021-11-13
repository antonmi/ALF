defmodule ALF.DSLTest do
  use ExUnit.Case, async: true

  alias ALF.Builder
  alias ALF.Components.{Stage, Switch, Clone, DeadEnd, GotoPoint, Goto}

  defmodule PipelineA do
    use ALF.DSL

    @components [
      stage(ModuleA),
      stage(StageA1, name: :custom_name),
      stage(:just_function),
      stage(:just_function, name: :custom_name)
    ]
  end

  defmodule PipelineB do
    use ALF.DSL

    @components [
      goto_point(:goto_point),
      clone(:clone, to: [stage(Mod1), dead_end(:dead_end)]),
      switch(:switch,
        partitions: %{
          part1: stages_from(PipelineA, opts: %{foo: :bar}),
          part2: [stage(ModInPart2)]
        },
        cond: :cond_function
      ),
      goto(:goto, to: :goto_point, if: :function, opts: [foo: :bar])
    ]
  end

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  describe "PipelineA" do
    test "build PipelineA", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineA.alf_components(), sup_pid)

      [one, two, three, four] = pipeline.components
      assert %Stage{name: ModuleA} = one
      assert %Stage{name: :custom_name} = two
      assert %Stage{name: :just_function} = three
      assert %Stage{name: :custom_name} = four
    end
  end

  describe "PipelineB" do
    test "build PipelineB", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineB.alf_components(), sup_pid)

      [goto_point, clone, switch, goto] = pipeline.components

      assert %GotoPoint{name: :goto_point} = goto_point
      assert %Clone{name: :clone, to: [_stage_mod1, dead_end]} = clone

      assert %Switch{
               name: :switch,
               partitions: %{
                 part1: [one, two, three, four],
                 part2: [_stage_in_part2]
               }
             } = switch

      assert %DeadEnd{name: :dead_end} = dead_end
      assert %Goto{name: :goto, to: :goto_point, opts: [foo: :bar]} = goto

      assert [
               %Stage{name: ModuleA, opts: [foo: :bar]},
               %Stage{name: :custom_name, opts: [foo: :bar]},
               %Stage{name: :just_function, opts: [foo: :bar]},
               %Stage{name: :custom_name, opts: [foo: :bar]}
             ] = [one, two, three, four]
    end
  end
end
