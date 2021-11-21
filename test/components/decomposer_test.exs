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

  def decomposer_function_list(datum, _opts) do
    [datum + 1, datum + 2, datum + 3]
  end

  def decomposer_function_tuple(datum, _opts) do
    {[datum + 1, datum + 2, datum + 3], datum + 100}
  end

  def build_decomposer(producer_pid, function) do
    %Decomposer{
      name: :decomposer,
      module: __MODULE__,
      function: function,
      pipeline_module: __MODULE__,
      subscribe_to: [{producer_pid, max_demand: 1}]
    }
  end

  def run_and_wait_for_ips(state, producer_pid, consumer_pid, stream_ref, ip_ref) do
    new_state = %{state | registry: %{stream_ref => %StreamRegistry{}}}
    Manager.__set_state__(EmptyPipeline, new_state)
    ip = %IP{datum: 1, manager_name: EmptyPipeline, stream_ref: stream_ref, ref: ip_ref}

    Manager.add_to_registry(EmptyPipeline, [ip], stream_ref)
    GenServer.cast(producer_pid, [ip])
    Process.sleep(10)

    TestConsumer.ips(consumer_pid)
  end

  describe "with list as return value" do
    setup %{producer_pid: producer_pid} do
      {:ok, pid} =
        Decomposer.start_link(build_decomposer(producer_pid, :decomposer_function_list))

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
      ip_ref = make_ref()
      [ip | _] = ips = run_and_wait_for_ips(state, producer_pid, consumer_pid, stream_ref, ip_ref)

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

  describe "with tuple as return value" do
    setup %{producer_pid: producer_pid} do
      {:ok, pid} =
        Decomposer.start_link(build_decomposer(producer_pid, :decomposer_function_tuple))

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
      ip_ref = make_ref()
      [ip | _] = ips = run_and_wait_for_ips(state, producer_pid, consumer_pid, stream_ref, ip_ref)

      assert Enum.count(ips) == 4

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

      original_ip = List.last(ips)

      assert %ALF.IP{
               datum: 101,
               decomposed: false,
               history: [decomposer: 1],
               manager_name: EmptyPipeline,
               recomposed: false,
               ref: ^ip_ref,
               stream_ref: ^stream_ref
             } = original_ip
    end
  end
end
