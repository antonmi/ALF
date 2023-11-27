defmodule ALF.BuilderTest do
  use ExUnit.Case, async: true

  alias ALF.Builder
  alias ALF.Components.{Stage, Switch, Broadcaster}

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

  defmodule PipelineWithBroadcaster do
    def alf_components do
      [
        %Broadcaster{name: :broadcaster},
        %Stage{name: :stage1, count: 2},
        %Stage{name: :stage2}
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

  describe "broadcaster" do
    test "build pipeline with broadcaster", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineWithBroadcaster, sup_pid, :pipeline)

      [broadcaster, _stage11, _stage12, stage2] = pipeline.components

      assert %Broadcaster{name: :broadcaster, pid: broadcaster_pid} = broadcaster
      assert is_pid(broadcaster_pid)
      assert %Stage{name: :stage2} = stage2
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

    test "build with spec_with_broadcaster" do
      [producer, broadcaster, stage1, stage2, consumer] =
        Builder.build_sync(PipelineWithBroadcaster, true)

      assert broadcaster.telemetry
      assert broadcaster.subscribed_to == [{producer.pid, :sync}]

      assert stage1.name == :stage1
      assert stage1.pid
      assert stage1.subscribed_to == [{broadcaster.pid, :sync}]

      assert consumer.subscribed_to == [{stage2.pid, :sync}]
    end
  end
end
