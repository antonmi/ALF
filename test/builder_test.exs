defmodule ALF.BuilderTest do
  use ExUnit.Case, async: true

  alias ALF.Builder
  alias ALF.Components.{Stage, Switch, Clone, DeadEnd}

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  defmodule SimplePipeline do
    def alf_components do
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
  end

  defmodule PipelineWithSwitch do
    def alf_components do
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
  end

  defmodule PipelineWithClone do
    def alf_components do
      [
        %Clone{
          name: :clone,
          to: [%Stage{name: :stage1}]
        },
        %Stage{name: :stage2}
      ]
    end
  end

  defmodule PipelineWithCloneAndDeadEnd do
    def alf_components do
      [
        %Clone{
          name: :clone,
          to: [%Stage{name: :stage1}, %DeadEnd{name: :dead_end}]
        },
        %Stage{name: :stage2}
      ]
    end
  end

  describe "simple pipeline" do
    test "build simple pipeline", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(SimplePipeline, sup_pid, :manager, :pipeline)

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

      assert stage.subscribe_to == [{pipeline.producer.pid, max_demand: 1, cancel: :transient}]

      assert Enum.count(components) == 3
      assert Enum.map(components, & &1.number) == [0, 1, 2]
    end
  end

  describe "switch" do
    test "build pipeline with switch", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineWithSwitch, sup_pid, :manager, :pipeline)

      switch = hd(pipeline.components)

      assert %Switch{
               function: :cond_function,
               name: :switch,
               pid: switch_pid,
               branches: branches
             } = switch

      assert is_pid(switch_pid)
      assert switch.subscribe_to == [{pipeline.producer.pid, max_demand: 1, cancel: :transient}]

      assert [
               %Stage{
                 name: :stage_in_part1,
                 subscribe_to: [
                   {^switch_pid, [max_demand: 1, cancel: :transient, partition: :part1]}
                 ]
               }
             ] = branches[:part1]

      assert [
               %Stage{
                 name: :stage_in_part2,
                 subscribe_to: [
                   {^switch_pid, [max_demand: 1, cancel: :transient, partition: :part2]}
                 ]
               }
             ] = branches[:part2]
    end
  end

  describe "clone" do
    test "build pipeline with clone", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineWithClone, sup_pid, :manager, :pipeline)

      [clone | [stage2]] = pipeline.components

      assert %Clone{
               name: :clone,
               pid: clone_pid,
               to: [to_stage]
             } = clone

      assert is_pid(clone_pid)

      assert %Stage{
               pid: stage1_pid,
               subscribe_to: [{^clone_pid, max_demand: 1, cancel: :transient}]
             } = to_stage

      assert Enum.member?(stage2.subscribe_to, {clone_pid, max_demand: 1, cancel: :transient})
      assert Enum.member?(stage2.subscribe_to, {stage1_pid, max_demand: 1, cancel: :transient})
    end

    test "build pipeline with clone and dead_end", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineWithCloneAndDeadEnd, sup_pid, :manager, :pipeline)

      [clone | [stage2]] = pipeline.components

      assert %Clone{
               name: :clone,
               pid: clone_pid,
               to: [to_stage, dead_end]
             } = clone

      assert is_pid(clone_pid)

      assert %Stage{
               pid: stage1_pid,
               subscribe_to: [{^clone_pid, max_demand: 1, cancel: :transient}]
             } = to_stage

      assert %DeadEnd{subscribe_to: [{^stage1_pid, max_demand: 1, cancel: :transient}]} = dead_end

      assert Enum.member?(stage2.subscribe_to, {clone_pid, max_demand: 1, cancel: :transient})
      refute Enum.member?(stage2.subscribe_to, {stage1_pid, max_demand: 1, cancel: :transient})
    end
  end

  describe "build_sync" do
    test "build with spec_simple_sync" do
      [producer, stage, consumer] = Builder.build_sync(SimplePipeline, true)
      assert producer.name == :producer
      assert is_reference(producer.pid)

      assert consumer.name == :consumer
      assert is_reference(consumer.pid)

      assert is_reference(stage.pid)
      assert stage.telemetry_enabled
    end

    test "build with spec_with_switch" do
      [_, switch, _] = Builder.build_sync(PipelineWithSwitch, true)

      assert switch.telemetry_enabled

      %{branches: %{part1: [stage1], part2: [stage2]}} = switch

      assert stage1.name == :stage_in_part1
      assert stage1.pid
      assert stage2.name == :stage_in_part2
      assert stage2.pid
    end

    test "build with spec_with_clone" do
      [_, clone, stage, _] = Builder.build_sync(PipelineWithClone, true)

      assert clone.telemetry_enabled
      [to_stage] = clone.to
      assert to_stage.name == :stage1
      assert to_stage.pid
      assert stage.name == :stage2
      assert stage.pid
    end

    test "spec_with_clone_and_dead_end" do
      [_, clone, _stage, _] = Builder.build_sync(PipelineWithCloneAndDeadEnd, true)
      assert [%Stage{}, %DeadEnd{}] = clone.to
    end
  end
end
