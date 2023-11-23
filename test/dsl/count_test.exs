defmodule ALF.DSLCountTest do
  use ExUnit.Case, async: true

  defmodule ManyComposers do
    use ALF.DSL

    @components [
      composer(:do_nothing, count: 2)
    ]

    def do_nothing(event, nil, _), do: {[event], nil}
  end

  describe "Composer" do
    setup do
      ManyComposers.start()
      Process.sleep(5)
      on_exit(&ManyComposers.stop/0)
    end

    test "count" do
      [producer, composer1, composer2, consumer] = ManyComposers.components()

      assert is_reference(composer1.stage_set_ref) and
               composer1.stage_set_ref == composer2.stage_set_ref

      producer_subs = Enum.map(producer.subscribers, fn {{pid, _ref}, _} -> pid end)
      assert Enum.member?(producer_subs, composer1.pid)
      assert Enum.member?(producer_subs, composer2.pid)

      producer_pid = producer.pid
      [{{^producer_pid, _ref}, _}] = composer1.subscribed_to
      [{{^producer_pid, _ref}, _}] = composer2.subscribed_to

      consumer_subs_to = Enum.map(consumer.subscribed_to, fn {{pid, _ref}, _} -> pid end)

      assert Enum.member?(consumer_subs_to, composer1.pid)
      assert Enum.member?(consumer_subs_to, composer2.pid)
    end
  end

  defmodule ManySwitches do
    use ALF.DSL

    @components [
      switch(:switch,
        count: 2,
        branches: %{
          true: [tbd()],
          false: [tbd()]
        }
      )
    ]

    def switch(event, _), do: rem(event, 2) == 0
  end

  describe "Switch" do
    setup do
      ManySwitches.start()
      Process.sleep(5)
      on_exit(&ManySwitches.stop/0)
    end

    test "count" do
      components = ManySwitches.components()
      producer = Enum.find(components, &(&1.type == :producer))
      switch1 = Enum.find(components, &(&1.type == :switch and &1.number == 0))
      switch2 = Enum.find(components, &(&1.type == :switch and &1.number == 1))

      assert is_reference(switch1.stage_set_ref) and
               switch1.stage_set_ref == switch2.stage_set_ref

      producer_subs = Enum.map(producer.subscribers, fn {{pid, _ref}, _} -> pid end)
      assert Enum.member?(producer_subs, switch1.pid)
      assert Enum.member?(producer_subs, switch2.pid)

      producer_pid = producer.pid
      [{{^producer_pid, _ref}, _}] = switch1.subscribed_to
      [{{^producer_pid, _ref}, _}] = switch2.subscribed_to

      tbds = Enum.filter(components, &(&1.type == :tbd))

      tbds_subscribed_to_pids =
        Enum.map(tbds, fn tbd ->
          [{{pid, _ref}, _}] = tbd.subscribed_to
          pid
        end)

      assert Enum.member?(tbds_subscribed_to_pids, switch1.pid)
      assert Enum.member?(tbds_subscribed_to_pids, switch2.pid)

      consumer = Enum.find(components, &(&1.type == :consumer))

      consumer_subs_to =
        consumer.subscribed_to
        |> Enum.map(fn {{pid, _ref}, _} -> pid end)
        |> Enum.sort()

      assert Enum.sort(Enum.map(tbds, & &1.pid)) == consumer_subs_to
    end
  end

  defmodule ManyGotos do
    use ALF.DSL

    @components [
      goto_point(:again, count: 2),
      stage(:add_one),
      goto(:if_less, to: :again, count: 2)
    ]

    def add_one(event, _), do: event + 1
    def if_less(event, _), do: event < 3
  end

  describe "Goto and GotoPoint" do
    setup do
      ManyGotos.start()
      Process.sleep(5)
      on_exit(&ManyGotos.stop/0)
    end

    test "count" do
      ManyGotos.components()

      components = ManyGotos.components()
      producer = Enum.find(components, &(&1.type == :producer))
      goto_point1 = Enum.find(components, &(&1.type == :goto_point and &1.number == 0))
      goto_point2 = Enum.find(components, &(&1.type == :goto_point and &1.number == 1))

      assert is_reference(goto_point1.stage_set_ref) and
               goto_point1.stage_set_ref == goto_point2.stage_set_ref

      producer_subs = Enum.map(producer.subscribers, fn {{pid, _ref}, _} -> pid end)
      assert Enum.member?(producer_subs, goto_point1.pid)
      assert Enum.member?(producer_subs, goto_point2.pid)

      goto1 = Enum.find(components, &(&1.type == :goto and &1.number == 0))
      goto2 = Enum.find(components, &(&1.type == :goto and &1.number == 1))
      assert is_reference(goto1.stage_set_ref) and goto1.stage_set_ref == goto2.stage_set_ref

      consumer = Enum.find(components, &(&1.type == :consumer))
      consumer_subs_to = Enum.map(consumer.subscribed_to, fn {{pid, _ref}, _} -> pid end)

      assert Enum.member?(consumer_subs_to, goto1.pid)
      assert Enum.member?(consumer_subs_to, goto2.pid)
    end
  end

  defmodule ManyDeadEnds do
    use ALF.DSL

    @components [
      dead_end(:the_end, count: 2)
    ]
  end

  describe "DeadEnd" do
    setup do
      ManyDeadEnds.start()
      Process.sleep(5)
      on_exit(&ManyDeadEnds.stop/0)
    end

    test "count" do
      components = ManyDeadEnds.components()
      producer = Enum.find(components, &(&1.type == :producer))
      dead_end1 = Enum.find(components, &(&1.type == :dead_end and &1.number == 0))
      dead_end2 = Enum.find(components, &(&1.type == :dead_end and &1.number == 1))

      assert is_reference(dead_end1.stage_set_ref) and
               dead_end2.stage_set_ref == dead_end2.stage_set_ref

      producer_subs = Enum.map(producer.subscribers, fn {{pid, _ref}, _} -> pid end)
      assert Enum.member?(producer_subs, dead_end1.pid)
      assert Enum.member?(producer_subs, dead_end2.pid)

      consumer = Enum.find(components, &(&1.type == :consumer))
      assert consumer.subscribed_to == []
    end
  end

  defmodule ManyPlugs do
    use ALF.DSL

    defmodule ManyTbds do
      use ALF.DSL

      @components [
        done(:done)
      ]
    end

    defmodule Adapter do
      def plug(event, _), do: event
      def unplug(event, _prev_event, _), do: event
    end

    @components (plug_with(Adapter, count: 2) do
                   stages_from(ManyTbds, count: 2)
                 end)
  end

  describe "Plug and Tbd" do
    setup do
      ManyPlugs.start()
      Process.sleep(5)
      on_exit(&ManyPlugs.stop/0)
    end

    test "count" do
      components = ManyPlugs.components()

      producer = Enum.find(components, &(&1.type == :producer))
      plug1 = Enum.find(components, &(&1.type == :plug and &1.number == 0))
      plug2 = Enum.find(components, &(&1.type == :plug and &1.number == 1))

      assert is_reference(plug1.stage_set_ref) and
               plug1.stage_set_ref == plug2.stage_set_ref

      producer_subs = Enum.map(producer.subscribers, fn {{pid, _ref}, _} -> pid end)
      assert Enum.member?(producer_subs, plug1.pid)
      assert Enum.member?(producer_subs, plug1.pid)

      dones = Enum.filter(components, &(&1.type == :done))
      assert length(dones) == 2

      unplug1 = Enum.find(components, &(&1.type == :unplug and &1.number == 0))
      unplug2 = Enum.find(components, &(&1.type == :unplug and &1.number == 1))

      assert is_reference(unplug1.stage_set_ref) and
               unplug1.stage_set_ref == unplug2.stage_set_ref

      consumer = Enum.find(components, &(&1.type == :consumer))
      consumer_subs_to = Enum.map(consumer.subscribed_to, fn {{pid, _ref}, _} -> pid end)

      assert Enum.member?(consumer_subs_to, unplug1.pid)
      assert Enum.member?(consumer_subs_to, unplug2.pid)
    end
  end
end
