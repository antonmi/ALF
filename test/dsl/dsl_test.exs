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
    def unplug(_datum, prev_event, _), do: prev_event
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
    test "build PipelineA", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineA.alf_components(), sup_pid, :manager, :pipeline)

      [one, two, three, four] = pipeline.components
      assert %Stage{name: ModuleA} = one
      assert %Stage{name: :custom_name} = two
      assert %Stage{name: :just_function} = three
      assert %Stage{name: :custom_name} = four
    end
  end

  describe "PipelineB" do
    test "build PipelineB", %{sup_pid: sup_pid} do
      {:ok, pipeline} = Builder.build(PipelineB.alf_components(), sup_pid, :manager, :pipeline)

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
      {:ok, pipeline} = Builder.build(PipelineC.alf_components(), sup_pid, :manager, :pipeline)

      [stage, plug, stage_in_plug, unplug, another_plug, _, _, _, last_stage, another_unplug] =
        pipeline.components

      assert %Stage{pid: stage_pid, name: ModuleA} = stage

      assert %Plug{
               name: MyAdapterModule,
               pid: plug_pid,
               subscribe_to: [{^stage_pid, max_demand: 1}]
             } = plug

      assert %Stage{pid: stage_pid, subscribe_to: [{^plug_pid, max_demand: 1}]} = stage_in_plug

      assert %Unplug{
               name: MyAdapterModule,
               pid: unplug_pid,
               subscribe_to: [{^stage_pid, max_demand: 1}]
             } = unplug

      assert %Plug{
               name: :another_plug,
               pid: _plug_pid,
               subscribe_to: [{^unplug_pid, max_demand: 1}]
             } = another_plug

      assert %Stage{pid: last_stage_pid, name: :custom_name} = last_stage

      assert %Unplug{
               name: :another_plug,
               pid: _plug_pid,
               subscribe_to: [{^last_stage_pid, max_demand: 1}]
             } = another_unplug
    end
  end

  describe "PipelineCompose" do
    test "build PipelineCompose", %{sup_pid: sup_pid} do
      {:ok, pipeline} =
        Builder.build(PipelineCompose.alf_components(), sup_pid, :manager, :pipeline)

      [decomposer, recomposer] = pipeline.components

      assert %Decomposer{
               module: PipelineCompose,
               function: :decomposer_function,
               name: :decomposer_function,
               opts: [foo: :bar],
               pid: decomposer_pid,
               pipe_module: ALF.DSLTest.PipelineCompose,
               pipeline_module: ALF.DSLTest.PipelineCompose,
               subscribe_to: [{producer_pid, [max_demand: 1]}],
               subscribers: []
             } = decomposer

      assert is_pid(decomposer_pid)
      assert is_pid(producer_pid)

      assert %Recomposer{
               module: PipelineCompose,
               function: :recomposer_function,
               name: :recomposer_function,
               opts: [foo: :bar],
               pid: recomposer_pid,
               pipe_module: ALF.DSLTest.PipelineCompose,
               pipeline_module: ALF.DSLTest.PipelineCompose,
               subscribe_to: [{^decomposer_pid, [max_demand: 1]}],
               subscribers: []
             } = recomposer

      assert is_pid(recomposer_pid)
    end
  end
end
