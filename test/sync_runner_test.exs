defmodule ALF.SyncRunnerTest do
  use ExUnit.Case, async: true

  alias ALF.{Builder, SyncRunner}
  alias ALF.Components.{Producer, Stage, Switch, Broadcaster, Consumer}

  defmodule Pipeline1 do
    def alf_components do
      [
        %Stage{name: :stage1},
        %Switch{
          name: :switch,
          branches: %{
            part1: [%Stage{name: :stage_in_part1}],
            part2: [
              %Broadcaster{name: :broadcaster},
              %Stage{name: :stage_after_broadcaster}
            ]
          },
          function: :cond_function
        },
        %Stage{name: :last_stage}
      ]
    end
  end

  setup do
    pipeline = Builder.build_sync(Pipeline1, true)

    [
      %Producer{pid: producer_pid},
      %Stage{name: :stage1, pid: stage1_pid},
      %Switch{
        name: :switch,
        pid: switch_stage1_pid,
        branches: %{
          part1: [%Stage{name: :stage_in_part1, pid: stage_in_part1_pid}],
          part2: [
            %Broadcaster{name: :broadcaster, pid: broadcaster_pid},
            %Stage{name: :stage_after_broadcaster, pid: stage_after_broadcaster_pid}
          ]
        },
        function: :cond_function
      },
      %Stage{name: :last_stage, pid: last_stage_pid},
      %Consumer{pid: consumer_pid}
    ] = pipeline

    %{
      producer_pid: producer_pid,
      pipeline: pipeline,
      stage1_pid: stage1_pid,
      switch_stage1_pid: switch_stage1_pid,
      stage_in_part1_pid: stage_in_part1_pid,
      broadcaster_pid: broadcaster_pid,
      stage_after_broadcaster_pid: stage_after_broadcaster_pid,
      last_stage_pid: last_stage_pid,
      consumer_pid: consumer_pid
    }
  end

  describe "path" do
    test "for producer_pid", %{
      pipeline: pipeline,
      producer_pid: producer_pid,
      stage1_pid: stage1_pid,
      switch_stage1_pid: switch_stage1_pid,
      last_stage_pid: last_stage_pid,
      consumer_pid: consumer_pid
    } do
      {path, true} = SyncRunner.path(pipeline, producer_pid)

      assert path == [
               producer_pid,
               stage1_pid,
               switch_stage1_pid,
               last_stage_pid,
               consumer_pid
             ]
    end

    test "for stage1_pid", %{
      pipeline: pipeline,
      stage1_pid: stage1_pid,
      switch_stage1_pid: switch_stage1_pid,
      last_stage_pid: last_stage_pid,
      consumer_pid: consumer_pid
    } do
      {path, true} = SyncRunner.path(pipeline, stage1_pid)

      assert path == [
               stage1_pid,
               switch_stage1_pid,
               last_stage_pid,
               consumer_pid
             ]
    end

    test "for switch_stage1_pid", %{
      pipeline: pipeline,
      stage_in_part1_pid: stage_in_part1_pid,
      last_stage_pid: last_stage_pid,
      consumer_pid: consumer_pid
    } do
      {path, true} = SyncRunner.path(pipeline, stage_in_part1_pid)
      assert path == [stage_in_part1_pid, last_stage_pid, consumer_pid]
    end

    test "for stage_after_broadcaster_pid", %{
      pipeline: pipeline,
      broadcaster_pid: broadcaster_pid,
      stage_after_broadcaster_pid: stage_after_broadcaster_pid,
      last_stage_pid: last_stage_pid,
      consumer_pid: consumer_pid
    } do
      {path, true} = SyncRunner.path(pipeline, broadcaster_pid)

      assert path == [
               broadcaster_pid,
               stage_after_broadcaster_pid,
               last_stage_pid,
               consumer_pid
             ]
    end

    test "for consumer_pid", %{pipeline: pipeline, consumer_pid: consumer_pid} do
      {path, true} = SyncRunner.path(pipeline, consumer_pid)
      assert path == [consumer_pid]
    end
  end

  describe "find_component/2" do
    test "find switch", %{pipeline: pipeline, switch_stage1_pid: switch_stage1_pid} do
      assert %Switch{name: :switch} = SyncRunner.find_component(pipeline, switch_stage1_pid)
    end

    test "find broadcaster", %{pipeline: pipeline, broadcaster_pid: broadcaster_pid} do
      assert %Broadcaster{name: :broadcaster} =
               SyncRunner.find_component(pipeline, broadcaster_pid)
    end
  end
end
