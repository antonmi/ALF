defmodule ALF.Components.SwitchTest do
  use ExUnit.Case, async: true
  alias ALF.{IP, TestProducer, TestConsumer}
  alias ALF.Components.Switch

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    %{producer_pid: producer_pid}
  end

  def cond_function(event, opts) do
    case event + opts[:add] do
      1 -> :part1
      2 -> :part2
    end
  end

  def build_switch(cond_function, producer_pid) do
    %Switch{
      name: :switch,
      branches: %{
        part1: [],
        part2: []
      },
      module: __MODULE__,
      subscribe_to: [{producer_pid, max_demand: 1}],
      function: cond_function,
      opts: %{add: 1}
    }
  end

  def setup_consumers(stage_pid) do
    {:ok, consumer1_pid} =
      TestConsumer.start_link(%TestConsumer{
        subscribe_to: [{stage_pid, partition: :part1, max_demand: 1}]
      })

    {:ok, consumer2_pid} =
      TestConsumer.start_link(%TestConsumer{
        subscribe_to: [{stage_pid, partition: :part2, max_demand: 1}]
      })

    {consumer1_pid, consumer2_pid}
  end

  describe "with function as &function/2" do
    setup %{producer_pid: producer_pid} do
      {:ok, pid} = Switch.start_link(build_switch(&cond_function/2, producer_pid))

      {consumer1_pid, consumer2_pid} = setup_consumers(pid)
      %{pid: pid, consumer1_pid: consumer1_pid, consumer2_pid: consumer2_pid}
    end

    test "test part1 path", %{
      producer_pid: producer_pid,
      consumer1_pid: consumer1_pid,
      consumer2_pid: consumer2_pid
    } do
      ip = %IP{event: 0}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(5)
      assert [%IP{event: 0}] = TestConsumer.ips(consumer1_pid)
      assert [] = TestConsumer.ips(consumer2_pid)
    end

    test "test part2 path", %{
      producer_pid: producer_pid,
      consumer1_pid: consumer1_pid,
      consumer2_pid: consumer2_pid
    } do
      ip = %IP{event: 1}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(5)
      assert [] = TestConsumer.ips(consumer1_pid)
      assert [%IP{event: 1}] = TestConsumer.ips(consumer2_pid)
    end

    test "set source_code", %{pid: pid} do
      %{source_code: source_code} = Switch.__state__(pid)
      assert String.starts_with?(source_code, "#Function")
    end
  end

  describe "with function as atom" do
    setup %{producer_pid: producer_pid} do
      {:ok, pid} = Switch.start_link(build_switch(:cond_function, producer_pid))

      {consumer1_pid, consumer2_pid} = setup_consumers(pid)
      %{pid: pid, consumer1_pid: consumer1_pid, consumer2_pid: consumer2_pid}
    end

    test "test part1 path", %{
      producer_pid: producer_pid,
      consumer1_pid: consumer1_pid,
      consumer2_pid: consumer2_pid
    } do
      ip = %IP{event: 0}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(5)
      assert [%IP{event: 0}] = TestConsumer.ips(consumer1_pid)
      assert [] = TestConsumer.ips(consumer2_pid)
    end

    test "test part2 path", %{
      producer_pid: producer_pid,
      consumer1_pid: consumer1_pid,
      consumer2_pid: consumer2_pid
    } do
      ip = %IP{event: 1}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(5)
      assert [] = TestConsumer.ips(consumer1_pid)
      assert [%IP{event: 1}] = TestConsumer.ips(consumer2_pid)
    end

    test "set source_code", %{pid: pid} do
      %{source_code: source_code} = Switch.__state__(pid)
      assert String.starts_with?(source_code, "def(cond_function(event, opts))")
    end
  end
end
