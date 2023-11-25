defmodule ALF.DSLTest do
  use ExUnit.Case, async: true

  alias ALF.Builder

  alias ALF.Components.{
    Stage,
    Switch,
    Clone,
    DeadEnd,
    GotoPoint,
    Goto,
    Plug,
    Unplug
  }

  defmodule PipelineA do
    use ALF.DSL

    @components [
      stage(ModuleA),
      stage(StageA1),
      stage(:just_function),
      stage(:just_another_function)
    ]
  end

  defmodule PipelineB do
    use ALF.DSL

    @components [
      goto_point(:goto_point),
      clone(:clone, to: [stage(Mod1), dead_end(:dead_end)]),
      switch(:cond_function,
        branches: %{
          part1: from(PipelineA, opts: %{foo: :bar}),
          part2: [stage(ModInPart2)]
        }
      ),
      goto(:goto, to: :goto_point, opts: [foo: :bar])
    ]
  end

  defmodule MyAdapterModule do
    def init(opts), do: opts
    def plug(event, _), do: event
    def unplug(_event, prev_event, _), do: prev_event
  end

  defmodule PipelineC do
    use ALF.DSL

    @components [
      stage(ModuleA),
      plug_with(MyAdapterModule) do
        [stage(StageA1)]
      end,
      plug_with(MyAdapterModule, opts: [a: :b]) do
        from(PipelineA)
      end
    ]
  end

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  describe "PipelineA" do
    test "build PipelineA with telemetry enabled", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineA, sup_pid, true)

      [one, two, three, four] = pipeline.components
      assert %Stage{name: ModuleA, telemetry: true} = one
      assert %Stage{name: StageA1, telemetry: true} = two
      assert %Stage{name: :just_function, telemetry: true} = three
      assert %Stage{name: :just_another_function, telemetry: true} = four
    end
  end

  describe "PipelineB" do
    test "build PipelineB", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineB, sup_pid, false)

      [goto_point, clone, switch, goto] = pipeline.components

      assert %GotoPoint{name: :goto_point} = goto_point
      assert %Clone{name: :clone, to: [_stage_mod1, dead_end]} = clone

      assert %Switch{
               name: :cond_function,
               branches: %{
                 part1: [one, two, three, four],
                 part2: [_stage_in_part2]
               }
             } = switch

      assert %DeadEnd{name: :dead_end} = dead_end
      assert %Goto{name: :goto, to: :goto_point, opts: [foo: :bar]} = goto

      assert [
               %Stage{name: ModuleA, opts: [foo: :bar]},
               %Stage{name: StageA1, opts: [foo: :bar]},
               %Stage{name: :just_function, opts: [foo: :bar]},
               %Stage{name: :just_another_function, opts: [foo: :bar]}
             ] = [one, two, three, four]
    end
  end

  describe "PipelineC" do
    test "build PipelineC", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineC, sup_pid, false)
      Process.sleep(10)

      [stage, plug, stage_in_plug, unplug, another_plug, _, _, _, last_stage, another_unplug] =
        pipeline.components

      assert %Stage{pid: stage_pid, name: ModuleA} = stage

      assert %Plug{
               name: MyAdapterModule,
               pid: plug_pid,
               subscribed_to: [{{^stage_pid, _ref}, _opts1}],
               subscribers: [{{stage_in_plug_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(plug.pid)

      assert %Stage{
               pid: ^stage_in_plug_pid,
               subscribed_to: [{{^plug_pid, _ref}, _opts1}],
               subscribers: [{{unplug_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(stage_in_plug.pid)

      assert %Unplug{
               name: MyAdapterModule,
               pid: ^unplug_pid,
               subscribed_to: [{{^stage_in_plug_pid, _ref}, _opts1}],
               subscribers: [{{_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(unplug.pid)

      assert %Plug{
               name: ALF.DSLTest.MyAdapterModule,
               pid: _plug_pid,
               subscribed_to: [{{^unplug_pid, _ref}, _opts1}],
               subscribers: [{{_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(another_plug.pid)

      assert %Stage{
               name: :just_another_function,
               pid: last_stage_pid,
               subscribed_to: [{{_pid, _ref}, _opts1}],
               subscribers: [{{another_unplug_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(last_stage.pid)

      assert %Unplug{
               name: ALF.DSLTest.MyAdapterModule,
               pid: ^another_unplug_pid,
               subscribed_to: [{{^last_stage_pid, _ref}, _opts1}],
               subscribers: [{{_consumer_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(another_unplug.pid)
    end
  end
end
