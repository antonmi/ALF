defmodule ALF.DSLTest do
  use ExUnit.Case

  alias ALF.{Builder, Stage, Switch, Clone, DeadEnd, Empty, Goto}

  defmodule PipelineA do
    use ALF.DSL

    @stages [
      stage(ModuleA),
      stage(StageA1, name: :custom_name),
      stage(:just_function),
      stage(:just_function, name: :custom_name),
    ]
  end

  defmodule PipelineB do
    use ALF.DSL

    @stages [
      empty(:empty),
      clone(:clone, to: [stage(Mod1), dead_end(:dead_end)]),
      switch(:switch,
        partitions: %{
          part1: stages_from(PipelineA, %{foo: :bar}),
          part2: [stage(ModInPart2)]
        },
        hash: :hash_function
      ),
      goto(:goto, to: :empty, if: :function)
    ]
  end

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  describe "PipelineA" do
    test "build PipelineA", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineA.stages(), sup_pid)

      [one, two, three, four] = pipeline.stages
      assert %Stage{name: ModuleA} = one
      assert %Stage{name: :custom_name} = two
      assert %Stage{name: :just_function} = three
      assert %Stage{name: :custom_name} = four
    end
  end

  describe "PipelineB" do
    test "build PipelineB", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineB.stages(), sup_pid)

      [empty, clone, switch, goto] = pipeline.stages

      assert %Empty{name: :empty} = empty
      assert %Clone{name: :clone, to: [stage_mod1, dead_end]} = clone
      assert %Switch{
        name: :switch,
        partitions: %{
          part1: [one, two, three, four],
          part2: [stage_in_part2]
        }
      } = switch

      assert %DeadEnd{name: :dead_end} = dead_end
      assert %Goto{name: :goto, to: :empty} = goto
      assert [
        %Stage{name: ModuleA, opts: %{foo: :bar}},
        %Stage{name: :custom_name, opts: %{foo: :bar}},
        %Stage{name: :just_function, opts: %{foo: :bar}},
        %Stage{name: :custom_name, opts: %{foo: :bar}}
      ] = [one, two, three, four]
    end
  end
end
