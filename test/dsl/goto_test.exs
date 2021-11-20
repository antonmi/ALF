# defmodule ALF.DSL.GotoTest do
#  use ExUnit.Case, async: true
#
#  alias ALF.Builder
#
#  alias ALF.Components.{
#    Stage,
#    Switch,
#    Clone,
#    DeadEnd,
#    GotoPoint,
#    Goto,
#    Plug,
#    Unplug,
#    Decomposer,
#    Recomposer
#    }
#
#
#  setup do
#    sup_pid = Process.whereis(ALF.DynamicSupervisor)
#    %{sup_pid: sup_pid}
#  end
#
#  describe "Switch with function as a name" do
#    defmodule PipelineSwitch1 do
#      use ALF.DSL
#
#      @components [
#        goto_point(:goto_point),
#        clone(:clone, to: [stage(Mod1), dead_end(:dead_end)]),
#        switch(:switch,
#          branches: %{
#            part1: stages_from(PipelineA, opts: %{foo: :bar}),
#            part2: [stage(ModInPart2)]
#          },
#          function: :cond_function
#        ),
#        goto(:goto, to: :goto_point, function: :function, opts: [foo: :bar])
#      ]
#    end
#
#    test "build PipelineSwitch1", %{sup_pid: sup_pid} do
#      {:ok, pipeline} = Builder.build(PipelineSwitch1.alf_components(), sup_pid)
#
#      [goto_point, clone, switch, goto] = pipeline.components
#
#      assert %GotoPoint{name: :goto_point} = goto_point
#      assert %Clone{name: :clone, to: [_stage_mod1, dead_end]} = clone
#
#      assert %Switch{
#               name: :switch,
#               branches: %{
#                 part1: [one, two, three, four],
#                 part2: [_stage_in_part2]
#               }
#             } = switch
#
#      assert %DeadEnd{name: :dead_end} = dead_end
#      assert %Goto{name: :goto, to: :goto_point, opts: [foo: :bar]} = goto
#
#      assert [
#               %Stage{name: ModuleA, opts: [foo: :bar]},
#               %Stage{name: :custom_name, opts: [foo: :bar]},
#               %Stage{name: :just_function, opts: [foo: :bar]},
#               %Stage{name: :custom_name, opts: [foo: :bar]}
#             ] = [one, two, three, four]
#    end
#  end
# end
