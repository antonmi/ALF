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

  def recomposer_function(event, prev_events, _opts) do
    sum = Enum.reduce(prev_events, 0, &(&1 + &2)) + event

    if sum > 5 do
      sum
    else
      :continue
    end
  end

  def recomposer_function_tuple(event, prev_events, _opts) do
    sum = Enum.reduce(prev_events, 0, &(&1 + &2)) + event

    if sum > 5 do
      {sum, [hd(prev_events)]}
    else
      :continue
    end
  end

  def build_recomposer(producer_pid, function) do
    %Recomposer{
      name: :recomposer,
      module: __MODULE__,
      function: function,
      pipeline_module: __MODULE__,
      subscribe_to: [{producer_pid, max_demand: 1}]
    }
  end

  def test_ips(stream_ref) do
    [
      %IP{
        event: 1,
        decomposed: true,
        manager_name: EmptyPipeline,
        stream_ref: stream_ref,
        ref: make_ref()
      },
      %IP{
        event: 2,
        decomposed: true,
        manager_name: EmptyPipeline,
        stream_ref: stream_ref,
        ref: make_ref()
      },
      %IP{
        event: 3,
        decomposed: true,
        manager_name: EmptyPipeline,
        stream_ref: stream_ref,
        ref: make_ref()
      }
    ]
  end

  def run_and_wait(state, producer_pid, consumer_pid, stream_ref) do
    ips = test_ips(stream_ref)
    decomposed = Enum.reduce(ips, %{}, &Map.put(&2, &1.ref, &1.event))
    new_state = %{state | registry: %{stream_ref => %StreamRegistry{decomposed: decomposed}}}
    Manager.__set_state__(EmptyPipeline, new_state)

    GenServer.cast(producer_pid, ips)
    Process.sleep(30)

    TestConsumer.ips(consumer_pid)
  end

  describe "when function returns an ip" do
    setup %{producer_pid: producer_pid} do
      {:ok, pid} = Recomposer.start_link(build_recomposer(producer_pid, :recomposer_function))

      {:ok, consumer_pid} =
        TestConsumer.start_link(%TestConsumer{subscribe_to: [{pid, max_demand: 1}]})

      %{pid: pid, consumer_pid: consumer_pid}
    end

    test "test recomposer", %{
      state: state,
      producer_pid: producer_pid,
      consumer_pid: consumer_pid
    } do
      stream_ref = make_ref()
      [ip] = run_and_wait(state, producer_pid, consumer_pid, stream_ref)

      assert %ALF.IP{
               event: 6,
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

  describe "when function returns a tuple" do
    setup %{producer_pid: producer_pid} do
      {:ok, pid} =
        Recomposer.start_link(build_recomposer(producer_pid, :recomposer_function_tuple))

      {:ok, consumer_pid} =
        TestConsumer.start_link(%TestConsumer{subscribe_to: [{pid, max_demand: 1}]})

      %{pid: pid, consumer_pid: consumer_pid}
    end

    test "test recomposer", %{
      state: state,
      producer_pid: producer_pid,
      consumer_pid: consumer_pid,
      pid: pid
    } do
      stream_ref = make_ref()
      [ip] = run_and_wait(state, producer_pid, consumer_pid, stream_ref)

      assert %ALF.IP{
               event: 6,
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

      state = Recomposer.__state__(pid)
      [collected_ip] = state.collected_ips

      assert %ALF.IP{
               event: 1,
               decomposed: false,
               history: [recomposer: 6, recomposer: 3],
               init_datum: 1,
               manager_name: ALF.Components.RecomposerTest.EmptyPipeline,
               plugs: %{},
               recomposed: true,
               ref: _ref,
               stream_ref: ^stream_ref
             } = collected_ip
    end
  end
end
