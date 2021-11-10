defmodule ALF.Components.StageTest do
  use ExUnit.Case, async: true
  alias ALF.{IP, TestProducer, TestConsumer}
  alias ALF.Components.Stage

  defmodule Component do
    def init(opts) do
      Map.put(opts, :foo, opts[:foo] <> "bar")
    end

    def call(datum, opts) do
      datum <> opts[:foo]
    end
  end

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    %{producer_pid: producer_pid}
  end

  def setup_stage(stage) do
    {:ok, pid} = Stage.start_link(stage)
    {:ok, consumer_pid} = TestConsumer.start_link(%TestConsumer{subscribe_to: [{pid, max_demand: 1}]})
    %{pid: pid, consumer_pid: consumer_pid}
  end

  setup %{producer_pid: producer_pid} do
    stage = %Stage{name: :test_stage, module: Component, function: :call, opts: %{foo: "foo"}, subscribe_to: [{producer_pid, max_demand: 1}]}
    setup_stage(stage)
  end

  test "init options", %{pid: pid} do
    %{opts: opts} = Stage.__state__(pid)
    assert opts[:foo] == "foobar"
  end

  test "call component", %{producer_pid: producer_pid, consumer_pid: consumer_pid} do
    ip = %IP{datum: "baz"}
    GenServer.cast(producer_pid, [ip])
    Process.sleep(1)
    [ip] = TestConsumer.ips(consumer_pid)
    assert ip.datum == "bazfoobar"
    assert ip.history == [{{:test_stage, 0}, "baz"}]
  end
end
