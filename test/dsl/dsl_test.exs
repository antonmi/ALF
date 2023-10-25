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
    Unplug,
    Decomposer,
    Recomposer
  }

  defmodule PipelineA do
    use ALF.DSL

    @components [
      stage(ModuleA),
      stage(StageA1, name: :custom_name),
      stage(:just_function),
      stage(:just_function, name: :custom_name)
    ]
  end

  defmodule PipelineB do
    use ALF.DSL

    @components [
      goto_point(:goto_point),
      clone(:clone, to: [stage(Mod1), dead_end(:dead_end)]),
      switch(:cond_function,
        branches: %{
          part1: stages_from(PipelineA, opts: %{foo: :bar}),
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
        [stage(StageA1, name: :custom_name)]
      end,
      plug_with(MyAdapterModule, opts: [a: :b], name: :another_plug) do
        stages_from(PipelineA)
      end
    ]
  end

  defmodule PipelineCompose do
    use ALF.DSL

    @components [
      decomposer(:decomposer_function, opts: [foo: :bar]),
      recomposer(:recomposer_function, opts: [foo: :bar])
    ]

    def decomposer_function(event, _) do
      [event + 1, event + 2, event + 3]
    end

    def recomposer_function(event, prev_events, _) do
      sum = Enum.reduce(prev_events, 0, &(&1 + &2)) + event

      if sum > 5 do
        sum
      else
        :continue
      end
    end
  end

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  describe "PipelineA" do
    test "build PipelineA with telemetry_enabled", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineA, sup_pid, true)

      [one, two, three, four] = pipeline.components
      assert %Stage{name: ModuleA, telemetry_enabled: true} = one
      assert %Stage{name: :custom_name, telemetry_enabled: true} = two
      assert %Stage{name: :just_function, telemetry_enabled: true} = three
      assert %Stage{name: :custom_name, telemetry_enabled: true} = four
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
               %Stage{name: :custom_name, opts: [foo: :bar]},
               %Stage{name: :just_function, opts: [foo: :bar]},
               %Stage{name: :custom_name, opts: [foo: :bar]}
             ] = [one, two, three, four]
    end
  end

  describe "PipelineC" do
    test "build PipelineC", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineC, sup_pid, false)

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
               name: :another_plug,
               pid: _plug_pid,
               subscribed_to: [{{^unplug_pid, _ref}, _opts1}],
               subscribers: [{{_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(another_plug.pid)

      assert %Stage{
               name: :custom_name,
               pid: last_stage_pid,
               subscribed_to: [{{_pid, _ref}, _opts1}],
               subscribers: [{{another_unplug_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(last_stage.pid)

      assert %Unplug{
               name: :another_plug,
               pid: ^another_unplug_pid,
               subscribed_to: [{{^last_stage_pid, _ref}, _opts1}],
               subscribers: [{{_consumer_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(another_unplug.pid)
    end
  end

  describe "PipelineCompose" do
    test "build PipelineCompose", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineCompose, sup_pid, false)

      Process.sleep(10)
      [decomposer, recomposer] = pipeline.components

      assert %Decomposer{
               module: PipelineCompose,
               function: :decomposer_function,
               name: :decomposer_function,
               opts: [foo: :bar],
               pid: decomposer_pid,
               pipeline_module: ALF.DSLTest.PipelineCompose,
               subscribed_to: [{{producer_pid, _ref1}, _opts1}],
               subscribers: [{{recomposer_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(decomposer.pid)

      assert is_pid(decomposer_pid)
      assert is_pid(producer_pid)
      assert is_pid(recomposer_pid)

      assert %Recomposer{
               module: PipelineCompose,
               function: :recomposer_function,
               name: :recomposer_function,
               opts: [foo: :bar],
               pid: ^recomposer_pid,
               pipeline_module: ALF.DSLTest.PipelineCompose,
               subscribed_to: [{{^decomposer_pid, _ref1}, _opts1}],
               subscribers: [{{consumer_pid, _ref2}, _opts2}]
             } = ALF.Components.Basic.__state__(recomposer.pid)

      assert is_pid(consumer_pid)
    end
  end
end
