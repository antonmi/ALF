defmodule ALF.Components.PlugTest do
  use ExUnit.Case, async: true
  alias ALF.{IP, TestProducer, TestConsumer}
  alias ALF.Components.{Plug, Unplug, Stage}

  defstruct [:number, :other]

  defmodule PlugAdapter do
    def init(opts) do
      Keyword.put(opts, :init, true)
    end

    def plug(event, opts) do
      assert opts[:init]
      event.number + opts[:foo]
    end

    def unplug(event, prev_event, opts) do
      assert opts[:init]
      Map.put(prev_event, :number, event)
    end
  end

  def build_plug(producer_pid) do
    %Plug{
      name: PlugAdapter,
      opts: [foo: 1],
      module: PlugAdapter,
      subscribe_to: [{producer_pid, max_demand: 1}]
    }
  end

  def build_unplug(stage_pid) do
    %Unplug{
      name: PlugAdapter,
      module: PlugAdapter,
      opts: [bar: 1],
      subscribe_to: [{stage_pid, max_demand: 1}]
    }
  end

  def build_stage(goto_point_pid) do
    %Stage{
      name: :test_stage,
      module: __MODULE__,
      function: :stage_function,
      subscribe_to: [{goto_point_pid, max_demand: 1}]
    }
  end

  def stage_function(event, _opts) do
    event + 100
  end

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    %{producer_pid: producer_pid}
  end

  def setup_pipeline(producer_pid) do
    {:ok, plug_pid} = Plug.start_link(build_plug(producer_pid))
    {:ok, stage_pid} = Stage.start_link(build_stage(plug_pid))
    {:ok, unplug_pid} = Unplug.start_link(build_unplug(stage_pid))

    {:ok, consumer_pid} =
      TestConsumer.start_link(%TestConsumer{subscribe_to: [{unplug_pid, max_demand: 1}]})

    consumer_pid
  end

  describe "event transformation" do
    setup %{producer_pid: producer_pid} do
      consumer_pid = setup_pipeline(producer_pid)

      %{consumer_pid: consumer_pid}
    end

    test "test plug/unplug with map", %{producer_pid: producer_pid, consumer_pid: consumer_pid} do
      ip = %IP{event: %{number: 1, other: :events}}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(20)
      [ip] = TestConsumer.ips(consumer_pid)
      assert ip.event == %{number: 102, other: :events}

      assert ip.history == [
               {ALF.Components.PlugTest.PlugAdapter, 102},
               {{:test_stage, 0}, 2},
               {ALF.Components.PlugTest.PlugAdapter, %{number: 1, other: :events}}
             ]
    end

    test "test plug/unplug with struct", %{producer_pid: producer_pid, consumer_pid: consumer_pid} do
      ip = %IP{event: %__MODULE__{number: 1, other: :events}}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(20)
      [ip] = TestConsumer.ips(consumer_pid)
      assert ip.event == %__MODULE__{number: 102, other: :events}

      assert ip.history == [
               {ALF.Components.PlugTest.PlugAdapter, 102},
               {{:test_stage, 0}, 2},
               {ALF.Components.PlugTest.PlugAdapter, %__MODULE__{number: 1, other: :events}}
             ]
    end
  end
end
