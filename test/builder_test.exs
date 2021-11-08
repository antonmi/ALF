defmodule ALF.BuilderTest do
  use ExUnit.Case

  alias ALF.{Builder, Stage, Switch, Clone, DeadEnd}


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
        },
      ]
    end

    test "build simple pipeline", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(spec_simple(), sup_pid)

      stages = pipeline.stages
      stage = hd(stages)

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

      assert Enum.count(stages) == 3
      assert Enum.map(stages, &(&1.number)) == [0, 1, 2]
    end
  end

  describe "switch" do
    def spec_with_switch do
      [
        %Switch{
          name: :switch,
          partitions: %{
            part1: [%Stage{name: :stage_in_part1}],
            part2: [%Stage{name: :stage_in_part2}]
          },
          hash: :hash_function
        },
      ]
    end

    test "build pipeline with switch", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(spec_with_switch(), sup_pid)

      switch = hd(pipeline.stages)

      assert %ALF.Switch{
        hash: :hash_function,
        name: :switch,
        pid: switch_pid,
        partitions: partitions,
      } = switch

      assert is_pid(switch_pid)
      assert switch.subscribe_to == [{pipeline.producer.pid, max_demand: 1}]

      assert [
               %ALF.Stage{
                 name: :stage_in_part1,
                 subscribe_to: [{^switch_pid, [max_demand: 1, partition: :part1]}]
               }
             ] = partitions[:part1]

      assert [
               %ALF.Stage{
                 name: :stage_in_part2,
                 subscribe_to: [{^switch_pid, [max_demand: 1, partition: :part2]}]
               }
             ] = partitions[:part2]
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
      {:ok, pipeline} = Builder.build(spec_with_clone(), sup_pid)

      [clone | [stage2]] = pipeline.stages

      assert %ALF.Clone{
               name: :clone,
               pid: clone_pid,
               to: [to_stage],
             } = clone

      assert is_pid(clone_pid)

      assert %Stage{pid: stage1_pid, subscribe_to: [{^clone_pid, max_demand: 1}]} = to_stage

      assert Enum.member?(stage2.subscribe_to, {clone_pid, max_demand: 1})
      assert Enum.member?(stage2.subscribe_to, {stage1_pid, max_demand: 1})
    end

    test "build pipeline with clone and dead_end", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(spec_with_clone_and_dead_end(), sup_pid)

      [clone | [stage2]] = pipeline.stages

      assert %ALF.Clone{
               name: :clone,
               pid: clone_pid,
               to: [to_stage, dead_end],
             } = clone

      assert is_pid(clone_pid)

      assert %Stage{pid: stage1_pid, subscribe_to: [{^clone_pid, max_demand: 1}]} = to_stage
      assert %DeadEnd{subscribe_to: [{^stage1_pid, max_demand: 1}]} = dead_end

      assert Enum.member?(stage2.subscribe_to, {clone_pid, max_demand: 1})
      refute Enum.member?(stage2.subscribe_to, {stage1_pid, max_demand: 1})
    end
  end
end
