defmodule ALF.PipelineTest do
  use ExUnit.Case

  alias ALF.{Builder, Pipeline}
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
      %Producer{},
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
      %Consumer{}
    ] = pipeline

    %{
      pipeline: pipeline,
      stage1_pid: stage1_pid,
      switch_stage1_pid: switch_stage1_pid,
      stage_in_part1_pid: stage_in_part1_pid,
      stage_in_part2_pid: stage_in_part2_pid,
      clone_pid: clone_pid,
      stage_in_clone_pid: stage_in_clone_pid,
      dead_end_pid: dead_end_pid,
      another_stage_in_part2_pid: another_stage_in_part2_pid,
      last_stage_pid: last_stage_pid
    }
  end

  describe "find_component_by_pid" do
    test "for stage1_pid", %{
      pipeline: pipeline,
      stage1_pid: stage1_pid
    } do
      assert %Stage{name: :stage1} = Pipeline.find_component_by_pid(pipeline, stage1_pid)
    end

    test "for switch_stage1_pid", %{
      pipeline: pipeline,
      stage_in_part1_pid: stage_in_part1_pid
    } do
      assert %Stage{name: :stage_in_part1} =
               Pipeline.find_component_by_pid(pipeline, stage_in_part1_pid)
    end

    test "for stage_in_part2_pid", %{
      pipeline: pipeline,
      stage_in_part2_pid: stage_in_part2_pid
    } do
      assert %Stage{name: :stage_in_part2} =
               Pipeline.find_component_by_pid(pipeline, stage_in_part2_pid)
    end

    test "for stage_in_clone_pid", %{
      pipeline: pipeline,
      stage_in_clone_pid: stage_in_clone_pid
    } do
      assert %Stage{name: :stage_in_clone} =
               Pipeline.find_component_by_pid(pipeline, stage_in_clone_pid)
    end

    test "for last_stage_pid", %{
      pipeline: pipeline,
      last_stage_pid: last_stage_pid
    } do
      assert %Stage{name: :last_stage} = Pipeline.find_component_by_pid(pipeline, last_stage_pid)
    end
  end
end
