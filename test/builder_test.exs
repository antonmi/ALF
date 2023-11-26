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

  defmodule PipelineWithCloneAndDeadEndReversed do
    def alf_components do
      [
        %Clone{
          name: :clone,
          to: [%Stage{name: :stage1}]
        },
        %Stage{name: :stage2},
        %DeadEnd{name: :dead_end}
      ]
    end
  end

  describe "simple pipeline" do
    test "build simple pipeline", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(SimplePipeline, sup_pid, :pipeline)

      components = pipeline.components
      stage = hd(components)
      stage = ALF.Components.Basic.__state__(stage.pid)

      assert %Stage{
               name: :stage1,
               pid: pid,
               module: Module,
               function: :function,
               opts: %{a: 1},
               count: 3,
               number: 0,
               subscribed_to: [{{producer_pid, _ref}, _opts}]
             } = stage

      assert is_pid(pid)

      assert producer_pid == pipeline.producer.pid

      assert Enum.count(components) == 3
      assert Enum.map(components, & &1.number) == [0, 1, 2]
    end
  end

  describe "switch" do
    test "build pipeline with switch", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineWithSwitch, sup_pid, :pipeline)

      switch = hd(pipeline.components)

      assert %Switch{
               function: :cond_function,
               name: :switch,
               pid: switch_pid,
               branches: branches
             } = switch

      assert is_pid(switch_pid)
      assert [%Stage{name: :stage_in_part1}] = branches[:part1]
      assert [%Stage{name: :stage_in_part2}] = branches[:part2]
    end
  end

  describe "clone" do
    test "build pipeline with clone", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineWithClone, sup_pid, :pipeline)

      [clone | [stage2]] = pipeline.components

      assert %Clone{
               name: :clone,
               pid: clone_pid,
               to: [to_stage]
             } = clone

      assert is_pid(clone_pid)

      assert %Stage{name: :stage1} = to_stage
      assert %Stage{name: :stage2} = stage2
    end

    test "build pipeline with clone and dead_end", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineWithCloneAndDeadEnd, sup_pid, :pipeline)

      [clone | [stage2]] = pipeline.components

      assert %Clone{
               name: :clone,
               pid: clone_pid,
               to: [to_stage, dead_end]
             } = clone

      assert is_pid(clone_pid)

      assert %Stage{name: :stage1} = to_stage
      assert %Stage{name: :stage2} = stage2

      assert %DeadEnd{} = dead_end
    end

    test "build pipeline with clone and dead_end, reversed", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineWithCloneAndDeadEndReversed, sup_pid, :pipeline)

      [clone | [stage2, dead_end]] = pipeline.components

      assert %Clone{
               name: :clone,
               pid: clone_pid,
               to: [to_stage]
             } = clone

      assert is_pid(clone_pid)

      assert %Stage{name: :stage1} = to_stage
      assert %Stage{name: :stage2} = stage2

      assert %DeadEnd{} = dead_end
    end
  end

  describe "build_sync" do
    test "build with spec_simple_sync" do
      [producer, stage, consumer] = Builder.build_sync(SimplePipeline, true)

      assert producer.name == :producer
      assert is_reference(producer.pid)

      assert is_reference(stage.pid)
      assert stage.telemetry
      assert stage.subscribed_to == [{producer.pid, :sync}]

      assert consumer.name == :consumer
      assert is_reference(consumer.pid)
      assert consumer.subscribed_to == [{stage.pid, :sync}]
    end

    test "build with spec_with_switch" do
      [producer, switch, consumer] = Builder.build_sync(PipelineWithSwitch, true)

      assert switch.telemetry
      assert switch.subscribed_to == [{producer.pid, :sync}]

      %{branches: %{part1: [stage1], part2: [stage2]}} = switch

      assert stage1.name == :stage_in_part1
      assert stage1.pid
      assert stage1.subscribed_to == [{switch.pid, :sync}]

      assert stage2.name == :stage_in_part2
      assert stage2.pid
      assert stage2.subscribed_to == [{switch.pid, :sync}]

      assert consumer.subscribed_to == [{stage1.pid, :sync}, {stage2.pid, :sync}]
    end

    test "build with spec_with_clone" do
      [producer, clone, stage, consumer] = Builder.build_sync(PipelineWithClone, true)

      assert clone.telemetry
      [to_stage] = clone.to
      assert clone.subscribed_to == [{producer.pid, :sync}]

      assert to_stage.name == :stage1
      assert to_stage.pid
      assert to_stage.subscribed_to == [{clone.pid, :sync}]

      assert stage.name == :stage2
      assert stage.pid
      assert stage.subscribed_to == [{clone.pid, :sync}]

      assert consumer.subscribed_to == [{stage.pid, :sync}]
    end

    test "spec_with_clone_and_dead_end" do
      [_, clone, _stage, _] = Builder.build_sync(PipelineWithCloneAndDeadEnd, true)
      assert [%Stage{}, %DeadEnd{}] = clone.to
    end
  end
end
