defmodule ALF.Components.DoneTest do
  use ExUnit.Case, async: true
  alias ALF.{IP, TestProducer, TestConsumer}
  alias ALF.Components.{Done, Stage}

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    %{producer_pid: producer_pid}
  end

  def done_function(event, _opts) do
    cond do
      event > 100 ->
        raise "error"

      event > 2 ->
        true

      true ->
        false
    end
  end

  def build_done(function) do
    %Done{
      name: :done,
      module: __MODULE__,
      function: function,
      pipeline_module: __MODULE__
    }
  end

  def stage_function(event, _) do
    event + 1
  end

  def build_stage() do
    %Stage{
      name: :test_stage,
      module: __MODULE__,
      function: :stage_function
    }
  end

  setup %{producer_pid: producer_pid} do
    {:ok, done_pid} = Done.start_link(build_done(:done_function))
    GenStage.sync_subscribe(done_pid, to: producer_pid, max_demand: 1, cancel: :temporary)
    {:ok, stage_pid} = Stage.start_link(build_stage())
    GenStage.sync_subscribe(stage_pid, to: done_pid, max_demand: 1, cancel: :temporary)

    {:ok, consumer_pid} =
      TestConsumer.start_link(%TestConsumer{subscribe_to: [{stage_pid, max_demand: 1}]})

    %{done_pid: done_pid, consumer_pid: consumer_pid}
  end

  test "test done when condition is false", %{
    producer_pid: producer_pid,
    consumer_pid: consumer_pid
  } do
    ip = %IP{event: 1, destination: self(), ref: make_ref()}
    GenServer.cast(producer_pid, [ip])
    Process.sleep(10)
    [ip] = TestConsumer.ips(consumer_pid)
    assert ip.event == 2
  end

  test "test done when condition is true", %{producer_pid: producer_pid} do
    ref = make_ref()
    ip = %IP{event: 3, destination: self(), ref: ref}
    GenServer.cast(producer_pid, [ip])

    receive do
      {^ref, ip} ->
        assert ip.event == 3
    end
  end

  test "when error in condition ", %{producer_pid: producer_pid} do
    ref = make_ref()
    ip = %IP{event: 101, destination: self(), ref: ref}
    GenServer.cast(producer_pid, [ip])

    receive do
      {^ref, error_ip} ->
        assert error_ip.error == %RuntimeError{message: "error"}
    end
  end
end
