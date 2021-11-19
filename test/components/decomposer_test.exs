defmodule ALF.Components.DecomposerTest do
  use ExUnit.Case, async: true
  alias ALF.{IP, Manager, Manager.StreamRegistry, TestProducer, TestConsumer}
  alias ALF.Components.Decomposer

  defmodule EmptyPipeline do
    use ALF.DSL

    @components []
  end

  setup do
    {:ok, producer_pid} = TestProducer.start_link([])
    Manager.start(EmptyPipeline)
    state = Manager.__state__(EmptyPipeline)
    %{state: state, producer_pid: producer_pid}
  end

  def decomposer_function(datum, _opts) do
    [datum + 1, datum + 2, datum + 3]
  end

  def build_decomposer(producer_pid) do
    %Decomposer{
      name: :decomposer,
      function: &decomposer_function/2,
      pipeline_module: __MODULE__,
      subscribe_to: [{producer_pid, max_demand: 1}]
    }
  end

  setup %{producer_pid: producer_pid} do
    {:ok, pid} = Decomposer.start_link(build_decomposer(producer_pid))

    {:ok, consumer_pid} =
      TestConsumer.start_link(%TestConsumer{subscribe_to: [{pid, max_demand: 1}]})

    %{pid: pid, consumer_pid: consumer_pid}
  end

  test "test decomposer", %{
    state: state,
    producer_pid: producer_pid,
    consumer_pid: consumer_pid
  } do
    stream_ref = make_ref()

    new_state = %{state | registry: %{stream_ref => %StreamRegistry{}}}
    Manager.__set_state__(EmptyPipeline, new_state)

    ip_ref = make_ref()
    ip = %IP{datum: 1, manager_name: EmptyPipeline, stream_ref: stream_ref, ref: ip_ref}
    Manager.add_to_registry(EmptyPipeline, [ip], stream_ref)

    GenServer.cast(producer_pid, [ip])

    Process.sleep(10)

    [ip | _] = ips = TestConsumer.ips(consumer_pid)
    assert Enum.count(ips) == 3

    assert %ALF.IP{
             datum: 2,
             decomposed: true,
             history: [decomposer: 1],
             init_datum: 2,
             manager_name: EmptyPipeline,
             recomposed: false,
             ref: ref,
             stream_ref: ^stream_ref
           } = ip

    refute ref == ip_ref

    state = Manager.__state__(EmptyPipeline)
    assert Enum.count(state.registry[stream_ref].decomposed) == 3
  end
end
