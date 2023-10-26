defmodule ALF.Components.StageTest do
  use ExUnit.Case, async: true
  alias ALF.{IP, TestProducer, TestConsumer}
  alias ALF.Components.Stage

  defmodule Component do
    def init(opts) do
      Keyword.put(opts, :foo, opts[:foo] <> "bar")
    end

    def call(event, opts) do
      event <> opts[:foo]
    end
  end

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    %{producer_pid: producer_pid}
  end

  def setup_stage(stage, producer_pid) do
    {:ok, pid} = Stage.start_link(stage)
    GenStage.sync_subscribe(pid, to: producer_pid, max_demand: 1, cancel: :temporary)

    {:ok, consumer_pid} =
      TestConsumer.start_link(%TestConsumer{subscribe_to: [{pid, max_demand: 1}]})

    %{pid: pid, consumer_pid: consumer_pid}
  end

  setup %{producer_pid: producer_pid} do
    stage = %Stage{
      name: :test_stage,
      module: Component,
      pipeline_module: __MODULE__,
      function: :call,
      opts: [foo: "foo"]
    }

    setup_stage(stage, producer_pid)
  end

  test "init options", %{pid: pid} do
    %{opts: opts} = Stage.__state__(pid)
    assert opts[:foo] == "foobar"
  end

  test "call component", %{producer_pid: producer_pid, consumer_pid: consumer_pid} do
    ip = %IP{event: "baz", debug: true}
    GenServer.cast(producer_pid, [ip])
    Process.sleep(10)
    [ip] = TestConsumer.ips(consumer_pid)
    assert ip.event == "bazfoobar"
    assert ip.history == [{{:test_stage, 0}, "baz"}]
  end

  test "set source_code", %{pid: pid} do
    %{source_code: source_code} = Stage.__state__(pid)
    assert String.starts_with?(source_code, "defmodule Component do")
  end

  test "when source_code is provided" do
    stage = %Stage{
      name: :test_stage2,
      module: Component,
      pipeline_module: __MODULE__,
      function: :call,
      opts: [foo: "foo"],
      source_code: "The source code"
    }

    %{pid: pid} = setup_stage(stage, self())

    %{source_code: source_code} = Stage.__state__(pid)
    assert source_code == "The source code"
  end
end
