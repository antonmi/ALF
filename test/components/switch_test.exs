defmodule ALF.Components.SwitchTest do
  use ExUnit.Case, async: true
  alias ALF.{IP, TestProducer, TestConsumer}
  alias ALF.Components.Switch

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    %{producer_pid: producer_pid}
  end

  def cond_function(datum, opts) do
    case datum + opts[:add] do
      1 -> :part1
      2 -> :part2
    end
  end

  def build_switch(cond_function, producer_pid) do
    %Switch{name: :switch,
      partitions: %{
        part1: [],
        part2: []
      },
      pipeline_module: __MODULE__,
      subscribe_to: [{producer_pid, max_demand: 1}],
      cond: cond_function,
      opts: %{add: 1}
    }
  end

  def setup_consumers(stage_pid) do
    {:ok, consumer1_pid} = TestConsumer.start_link(
      %TestConsumer{subscribe_to: [{stage_pid, partition: :part1, max_demand: 1}]}
    )
    {:ok, consumer2_pid} = TestConsumer.start_link(
      %TestConsumer{subscribe_to: [{stage_pid, partition: :part2, max_demand: 1}]}
    )
    {consumer1_pid, consumer2_pid}
  end

  describe "with cond as &function/2" do
    setup %{producer_pid: producer_pid} do
      {:ok, pid} = Switch.start_link(
        build_switch(&cond_function/2, producer_pid)
      )

      {consumer1_pid, consumer2_pid} = setup_consumers(pid)
      %{pid: pid, consumer1_pid: consumer1_pid, consumer2_pid: consumer2_pid}
    end

    test "test part1 path", %{producer_pid: producer_pid, consumer1_pid: consumer1_pid, consumer2_pid: consumer2_pid} do
      ip = %IP{datum: 0}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(1)
      assert [%IP{datum: 0}] = TestConsumer.ips(consumer1_pid)
      assert [] = TestConsumer.ips(consumer2_pid)
    end

    test "test part2 path", %{producer_pid: producer_pid, consumer1_pid: consumer1_pid, consumer2_pid: consumer2_pid} do
      ip = %IP{datum: 1}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(1)
      assert [] = TestConsumer.ips(consumer1_pid)
      assert [%IP{datum: 1}] = TestConsumer.ips(consumer2_pid)
    end
  end

  describe "with cond function" do
    setup %{producer_pid: producer_pid} do
      {:ok, pid} = Switch.start_link(build_switch(:cond_function, producer_pid))

      {consumer1_pid, consumer2_pid} = setup_consumers(pid)
      %{pid: pid, consumer1_pid: consumer1_pid, consumer2_pid: consumer2_pid}
    end

    test "test part1 path", %{producer_pid: producer_pid, consumer1_pid: consumer1_pid, consumer2_pid: consumer2_pid} do
      ip = %IP{datum: 0}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(1)
      assert [%IP{datum: 0}] = TestConsumer.ips(consumer1_pid)
      assert [] = TestConsumer.ips(consumer2_pid)
    end

    test "test part2 path", %{producer_pid: producer_pid, consumer1_pid: consumer1_pid, consumer2_pid: consumer2_pid} do
      ip = %IP{datum: 1}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(1)
      assert [] = TestConsumer.ips(consumer1_pid)
      assert [%IP{datum: 1}] = TestConsumer.ips(consumer2_pid)
    end
  end
end
