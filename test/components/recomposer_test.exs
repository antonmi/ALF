defmodule ALF.Components.RecomposerTest do
  use ExUnit.Case, async: true
  alias ALF.{IP, Manager, Manager.StreamRegistry, TestProducer, TestConsumer}
  alias ALF.Components.Recomposer

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

  def recomposer_function(datum, prev_data, _opts) do
    sum = Enum.reduce(prev_data, 0, &(&1 + &2)) + datum

    if sum > 5 do
      sum
    else
      :continue
    end
  end

  def build_recomposer(producer_pid) do
    %Recomposer{
      name: :recomposer,
      function: &recomposer_function/3,
      pipeline_module: __MODULE__,
      subscribe_to: [{producer_pid, max_demand: 1}]
    }
  end

  setup %{producer_pid: producer_pid} do
    {:ok, pid} = Recomposer.start_link(build_recomposer(producer_pid))

    {:ok, consumer_pid} =
      TestConsumer.start_link(%TestConsumer{subscribe_to: [{pid, max_demand: 1}]})

    %{pid: pid, consumer_pid: consumer_pid}
  end

  def test_ips(stream_ref) do
    [
      %IP{
        datum: 1,
        decomposed: true,
        manager_name: EmptyPipeline,
        stream_ref: stream_ref,
        ref: make_ref()
      },
      %IP{
        datum: 2,
        decomposed: true,
        manager_name: EmptyPipeline,
        stream_ref: stream_ref,
        ref: make_ref()
      },
      %IP{
        datum: 3,
        decomposed: true,
        manager_name: EmptyPipeline,
        stream_ref: stream_ref,
        ref: make_ref()
      }
    ]
  end

  test "test recomposer", %{
    state: state,
    producer_pid: producer_pid,
    consumer_pid: consumer_pid
  } do
    stream_ref = make_ref()
    ips = test_ips(stream_ref)
    decomposed = Enum.reduce(ips, %{}, &Map.put(&2, &1.ref, &1.datum))
    new_state = %{state | registry: %{stream_ref => %StreamRegistry{decomposed: decomposed}}}
    Manager.__set_state__(EmptyPipeline, new_state)

    GenServer.cast(producer_pid, ips)
    Process.sleep(20)

    [ip] = TestConsumer.ips(consumer_pid)

    assert %ALF.IP{
             datum: 6,
             decomposed: false,
             history: [recomposer: 3],
             init_datum: 6,
             manager_name: EmptyPipeline,
             recomposed: true,
             ref: ref,
             stream_ref: ^stream_ref
           } = ip

    assert is_reference(ref)

    state = Manager.__state__(EmptyPipeline)
    assert state.registry[stream_ref].decomposed == %{}
    assert state.registry[stream_ref].recomposed[ref] == 6
  end
end
