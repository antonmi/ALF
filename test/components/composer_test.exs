defmodule ALF.Components.ComposerTest do
  use ExUnit.Case
  alias ALF.{IP, TestProducer, TestConsumer}
  alias ALF.Components.Composer

  defmodule Comp do
    def init(opts) do
      opts
      |> Keyword.put(:foo, opts[:foo] <> "bar")
    end

    def call(event, acc, opts) do
      case event do
        "foo" ->
          {[event <> opts[:foo]], acc}

        "foofoo" ->
          {[event, event], acc}

        "init_acc" ->
          {[], 0}

        "inc" ->
          {[event <> "-#{acc}"], acc + 1}

        "wrong_return" ->
          event

        "error" ->
          raise "error"
      end
    end
  end

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    %{producer_pid: producer_pid}
  end

  def setup_composer(composer, producer_pid) do
    {:ok, pid} = Composer.start_link(composer)
    GenStage.sync_subscribe(pid, to: producer_pid, max_demand: 1, cancel: :temporary)

    {:ok, consumer_pid} =
      TestConsumer.start_link(%TestConsumer{subscribe_to: [{pid, max_demand: 1}]})

    %{pid: pid, consumer_pid: consumer_pid}
  end

  setup %{producer_pid: producer_pid} do
    composer = %Composer{
      type: :composer,
      name: :test_composer1,
      module: Comp,
      pipeline_module: __MODULE__,
      function: :call,
      telemetry: Enum.random([true, false]),
      opts: [foo: "foo"]
    }

    setup_composer(composer, producer_pid)
  end

  test "init options", %{pid: pid} do
    %{opts: opts} = Composer.__state__(pid)
    assert opts[:foo] == "foobar"
  end

  describe "call composer" do
    test "call with foo", %{producer_pid: producer_pid, consumer_pid: consumer_pid} do
      ip = %IP{event: "foo", debug: true}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(10)
      [ip] = TestConsumer.ips(consumer_pid)
      assert ip.composed
      assert ip.event == "foofoobar"
      assert ip.history == [{{:test_composer1, 0}, "foo"}]
    end

    test "call with foofoo", %{producer_pid: producer_pid, consumer_pid: consumer_pid} do
      ip = %IP{event: "foofoo", debug: true}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(10)
      [ip1, ip2] = TestConsumer.ips(consumer_pid)
      assert ip1.composed
      assert ip1.event == "foofoo"
      assert ip2.composed
      assert ip2.event == "foofoo"
    end

    test "call with acc", %{producer_pid: producer_pid, consumer_pid: consumer_pid} do
      ip = %IP{event: "init_acc", debug: true}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(10)
      [] = TestConsumer.ips(consumer_pid)

      ips = [%IP{event: "inc", debug: true}, %IP{event: "inc", debug: true}]
      GenServer.cast(producer_pid, ips)
      Process.sleep(10)
      [ip1, ip2] = TestConsumer.ips(consumer_pid)
      assert ip1.event == "inc-0"
      assert ip2.event == "inc-1"
    end

    test "call with error", %{producer_pid: producer_pid, consumer_pid: consumer_pid} do
      ip = %IP{event: "error", debug: true, destination: consumer_pid}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(10)

      assert [%ALF.ErrorIP{error: %RuntimeError{message: "error"}}] =
               TestConsumer.ips(consumer_pid)
    end

    test "call with wrong_return", %{producer_pid: producer_pid, consumer_pid: consumer_pid} do
      ip = %IP{event: "wrong_return", debug: true, destination: consumer_pid}
      GenServer.cast(producer_pid, [ip])
      Process.sleep(10)
      [error_ip] = TestConsumer.ips(consumer_pid)

      assert error_ip.error ==
               "Composer \"test_composer1\" must return the {[event], acc} tuple. Got \"wrong_return\""
    end
  end

  test "set source_code", %{pid: pid} do
    %{source_code: source_code} = Composer.__state__(pid)
    assert String.starts_with?(source_code, "defmodule Comp do")
  end

  test "when source_code is provided" do
    composer = %Composer{
      name: :test_composer2,
      module: Comp,
      pipeline_module: __MODULE__,
      function: :call,
      opts: [foo: "foo"],
      source_code: "The source code"
    }

    %{pid: pid} = setup_composer(composer, self())

    %{source_code: source_code} = Composer.__state__(pid)
    assert source_code == "The source code"
  end
end
