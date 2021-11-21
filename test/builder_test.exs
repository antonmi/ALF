defmodule ALF.BuilderTest do
  use ExUnit.Case, async: true

  alias ALF.Builder
  alias ALF.Components.{Stage, Switch, Clone, DeadEnd}

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  describe "simple pipeline" do
    def spec_simple do
      [
        %Stage{
          name: :stage1,
          count: 3,
          module: Module,
          function: :function,
          opts: %{a: 1}
        }
      ]
    end

    test "build simple pipeline", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(spec_simple(), sup_pid, :manager_name)

      components = pipeline.components
      stage = hd(components)

      assert %Stage{
               name: :stage1,
               count: 3,
               module: Module,
               function: :function,
               opts: %{a: 1},
               pid: pid
             } = stage

      assert is_pid(pid)

      assert stage.subscribe_to == [{pipeline.producer.pid, max_demand: 1}]

      assert Enum.count(components) == 3
      assert Enum.map(components, & &1.number) == [0, 1, 2]
    end
  end

  describe "switch" do
    def spec_with_switch do
      [
        %Switch{
          name: :switch,
          branches: %{
            part1: [%Stage{name: :stage_in_part1}],
            part2: [%Stage{name: :stage_in_part2}]
          },
          function: :cond_function
        }
      ]
    end

    test "build pipeline with switch", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(spec_with_switch(), sup_pid, :manager_name)

      switch = hd(pipeline.components)

      assert %Switch{
               function: :cond_function,
               name: :switch,
               pid: switch_pid,
               branches: branches
             } = switch

      assert is_pid(switch_pid)
      assert switch.subscribe_to == [{pipeline.producer.pid, max_demand: 1}]

      assert [
               %Stage{
                 name: :stage_in_part1,
                 subscribe_to: [{^switch_pid, [max_demand: 1, partition: :part1]}]
               }
             ] = branches[:part1]

      assert [
               %Stage{
                 name: :stage_in_part2,
                 subscribe_to: [{^switch_pid, [max_demand: 1, partition: :part2]}]
               }
             ] = branches[:part2]
    end
  end

  describe "clone" do
    def spec_with_clone do
      [
        %Clone{
          name: :clone,
          to: [%Stage{name: :stage1}]
        },
        %Stage{name: :stage2}
      ]
    end

    def spec_with_clone_and_dead_end do
      [
        %Clone{
          name: :clone,
          to: [%Stage{name: :stage1}, %DeadEnd{name: :dead_end}]
        },
        %Stage{name: :stage2}
      ]
    end

    test "build pipeline with clone", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(spec_with_clone(), sup_pid, :manager_name)

      [clone | [stage2]] = pipeline.components

      assert %Clone{
               name: :clone,
               pid: clone_pid,
               to: [to_stage]
             } = clone

      assert is_pid(clone_pid)

      assert %Stage{pid: stage1_pid, subscribe_to: [{^clone_pid, max_demand: 1}]} = to_stage

      assert Enum.member?(stage2.subscribe_to, {clone_pid, max_demand: 1})
      assert Enum.member?(stage2.subscribe_to, {stage1_pid, max_demand: 1})
    end

    test "build pipeline with clone and dead_end", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(spec_with_clone_and_dead_end(), sup_pid, :manager_name)

      [clone | [stage2]] = pipeline.components

      assert %Clone{
               name: :clone,
               pid: clone_pid,
               to: [to_stage, dead_end]
             } = clone

      assert is_pid(clone_pid)

      assert %Stage{pid: stage1_pid, subscribe_to: [{^clone_pid, max_demand: 1}]} = to_stage
      assert %DeadEnd{subscribe_to: [{^stage1_pid, max_demand: 1}]} = dead_end

      assert Enum.member?(stage2.subscribe_to, {clone_pid, max_demand: 1})
      refute Enum.member?(stage2.subscribe_to, {stage1_pid, max_demand: 1})
    end
  end
end
