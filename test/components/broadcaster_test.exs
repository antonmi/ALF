defmodule ALF.Components.BroadcasterTest do
  use ExUnit.Case, async: true
  alias ALF.{IP, TestProducer, TestConsumer}
  alias ALF.Components.Broadcaster

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    %{producer_pid: producer_pid}
  end

  def build_broadcaster() do
    %Broadcaster{
      name: :broadcaster,
      pipeline_module: __MODULE__
    }
  end

  def setup_consumers(stage_pid) do
    {:ok, consumer1_pid} =
      TestConsumer.start_link(%TestConsumer{subscribe_to: [{stage_pid, max_demand: 1}]})

    {:ok, consumer2_pid} =
      TestConsumer.start_link(%TestConsumer{subscribe_to: [{stage_pid, max_demand: 1}]})

    {consumer1_pid, consumer2_pid}
  end

  setup %{producer_pid: producer_pid} do
    {:ok, pid} = Broadcaster.start_link(build_broadcaster())

    GenStage.sync_subscribe(pid, to: producer_pid, max_demand: 1, cancel: :temporary)

    {consumer1_pid, consumer2_pid} = setup_consumers(pid)
    %{pid: pid, consumer1_pid: consumer1_pid, consumer2_pid: consumer2_pid}
  end

  test "test broadcaster", %{
    producer_pid: producer_pid,
    consumer1_pid: consumer1_pid,
    consumer2_pid: consumer2_pid
  } do
    ip = %IP{event: 1, destination: self(), ref: make_ref()}
    GenServer.cast(producer_pid, [ip])
    Process.sleep(10)
    assert [%IP{event: 1}] = TestConsumer.ips(consumer1_pid)
    assert [%IP{event: 1}] = TestConsumer.ips(consumer2_pid)
  end
end
