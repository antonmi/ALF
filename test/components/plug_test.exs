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

  def build_plug() do
    %Plug{
      name: PlugAdapter,
      opts: [foo: 1],
      module: PlugAdapter
    }
  end

  def build_unplug do
    %Unplug{
      name: PlugAdapter,
      module: PlugAdapter,
      opts: [bar: 1]
    }
  end

  def build_stage do
    %Stage{
      name: :test_stage,
      module: __MODULE__,
      function: :stage_function
    }
  end

  def stage_function(event, _) do
    event + 100
  end

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    %{producer_pid: producer_pid}
  end

  def setup_pipeline(producer_pid) do
    {:ok, plug_pid} = Plug.start_link(build_plug())
    GenStage.sync_subscribe(plug_pid, to: producer_pid, max_demand: 1, cancel: :temporary)
    {:ok, stage_pid} = Stage.start_link(build_stage())
    GenStage.sync_subscribe(stage_pid, to: plug_pid, max_demand: 1, cancel: :temporary)
    {:ok, unplug_pid} = Unplug.start_link(build_unplug())
    GenStage.sync_subscribe(unplug_pid, to: stage_pid, max_demand: 1, cancel: :temporary)

    {:ok, consumer_pid} =
      TestConsumer.start_link(%TestConsumer{subscribe_to: [{unplug_pid, max_demand: 1}]})

    {consumer_pid, plug_pid}
  end

  describe "event transformation" do
    setup %{producer_pid: producer_pid} do
      {consumer_pid, plug_pid} = setup_pipeline(producer_pid)

      %{consumer_pid: consumer_pid, plug_pid: plug_pid}
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

    test "set source_code", %{plug_pid: plug_pid} do
      %{source_code: source_code} = Plug.__state__(plug_pid)
      assert String.starts_with?(source_code, "def plug(event, opts)")
    end
  end
end
