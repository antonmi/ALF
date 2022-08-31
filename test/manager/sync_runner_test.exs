defmodule ALF.Manager.SyncRunnerTest do
  use ExUnit.Case

  alias ALF.{Builder, Manager.SyncRunner}
  alias ALF.Components.{Producer, Stage, Switch, Clone, DeadEnd, Consumer}

  defmodule Pipeline1 do
    def alf_components do
      [
        %Stage{name: :stage1},
        %Switch{
          name: :switch,
          branches: %{
            part1: [%Stage{name: :stage_in_part1}],
            part2: [
              %Stage{name: :stage_in_part2},
              %Clone{
                name: :clone,
                to: [%Stage{name: :stage_in_clone}, %DeadEnd{name: :dead_end}]
              },
              %Stage{name: :another_stage_in_part2}
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
            %Stage{name: :stage_in_part2, pid: stage_in_part2_pid},
            %Clone{
              name: :clone,
              pid: clone_pid,
              to: [
                %Stage{name: :stage_in_clone, pid: stage_in_clone_pid},
                %DeadEnd{name: :dead_end, pid: dead_end_pid}
              ]
            },
            %Stage{name: :another_stage_in_part2, pid: another_stage_in_part2_pid}
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
      stage_in_part2_pid: stage_in_part2_pid,
      clone_pid: clone_pid,
      stage_in_clone_pid: stage_in_clone_pid,
      dead_end_pid: dead_end_pid,
      another_stage_in_part2_pid: another_stage_in_part2_pid,
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

    test "for stage_in_part2_pid", %{
      pipeline: pipeline,
      stage_in_part2_pid: stage_in_part2_pid,
      clone_pid: clone_pid,
      another_stage_in_part2_pid: another_stage_in_part2_pid,
      last_stage_pid: last_stage_pid,
      consumer_pid: consumer_pid
    } do
      {path, true} = SyncRunner.path(pipeline, stage_in_part2_pid)

      assert path == [
               stage_in_part2_pid,
               clone_pid,
               another_stage_in_part2_pid,
               last_stage_pid,
               consumer_pid
             ]
    end

    test "for stage_in_clone_pid", %{
      pipeline: pipeline,
      stage_in_clone_pid: stage_in_clone_pid,
      dead_end_pid: dead_end_pid,
      another_stage_in_part2_pid: another_stage_in_part2_pid,
      last_stage_pid: last_stage_pid,
      consumer_pid: consumer_pid
    } do
      {path, true} = SyncRunner.path(pipeline, stage_in_clone_pid)

      assert path == [
               stage_in_clone_pid,
               dead_end_pid,
               another_stage_in_part2_pid,
               last_stage_pid,
               consumer_pid
             ]
    end
  end

  describe "find_component/2" do
    test "find switch", %{pipeline: pipeline, switch_stage1_pid: switch_stage1_pid} do
      assert %Switch{name: :switch} = SyncRunner.find_component(pipeline, switch_stage1_pid)
    end

    test "find clone", %{pipeline: pipeline, clone_pid: clone_pid} do
      assert %Clone{name: :clone} = SyncRunner.find_component(pipeline, clone_pid)
    end

    test "find dead_end", %{pipeline: pipeline, dead_end_pid: dead_end_pid} do
      assert %DeadEnd{name: :dead_end} = SyncRunner.find_component(pipeline, dead_end_pid)
    end
  end
end
