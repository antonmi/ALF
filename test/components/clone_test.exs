defmodule ALF.Components.CloneTest do
  use ExUnit.Case, async: true
  alias ALF.{IP, TestProducer, TestConsumer}
  alias ALF.Components.Clone

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    %{producer_pid: producer_pid}
  end

  def build_clone(producer_pid) do
    %Clone{
      name: :clone,
      to: [],
      pipeline_module: __MODULE__,
      subscribe_to: [{producer_pid, max_demand: 1}]
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
    {:ok, pid} = Clone.start_link(build_clone(producer_pid))

    {consumer1_pid, consumer2_pid} = setup_consumers(pid)
    %{pid: pid, consumer1_pid: consumer1_pid, consumer2_pid: consumer2_pid}
  end

  test "test clone", %{
    producer_pid: producer_pid,
    consumer1_pid: consumer1_pid,
    consumer2_pid: consumer2_pid
  } do
    ip = %IP{datum: 1}
    GenServer.cast(producer_pid, [ip])
    Process.sleep(1)
    assert [%IP{datum: 1}] = TestConsumer.ips(consumer1_pid)
    assert [%IP{datum: 1}] = TestConsumer.ips(consumer2_pid)
  end
end
