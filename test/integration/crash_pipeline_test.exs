defmodule ALF.CrashPipelineTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias ALF.Manager

  defmodule SimplePipelineToCrash do
    use ALF.DSL

    @sleep 1

    @components [
      stage(:foo),
      stage(:bar),
      stage(:baz)
    ]

    def foo(event, _opts) do
      Process.sleep(@sleep)
      "#{event}-foo"
    end

    def bar(event, _opts) do
      Process.sleep(@sleep)
      "#{event}-bar"
    end

    def baz(event, _opts) do
      Process.sleep(@sleep)
      "#{event}-baz"
    end
  end

  def run_kill_task(state, milliseconds) do
    component_to_kill = Enum.find(state.components, &(&1.name == :bar))
    pid = component_to_kill.pid

    Task.async(fn ->
      Process.sleep(milliseconds)
      Process.exit(pid, :kill)
    end)
  end

  describe "kill stage" do
    setup do
      Manager.start(SimplePipelineToCrash)

      on_exit(fn ->
        Manager.stop(SimplePipelineToCrash)
      end)

      state = Manager.__state__(SimplePipelineToCrash)
      %{state: state}
    end

    test "with one stream", %{state: state} do
      run_kill_task(state, 10)

      assert capture_log(fn ->
               results =
                 0..9
                 |> Manager.stream_to(SimplePipelineToCrash)
                 |> Enum.to_list()

               state = Manager.__state__(SimplePipelineToCrash)
               in_progress = hd(Map.values(state.registry_dump)).in_progress
               assert Enum.count(in_progress) + Enum.count(results) == 10
             end) =~ "Last message: {:DOWN, "
    end

    test "with several streams", %{state: state} do
      run_kill_task(state, 50)

      assert capture_log(fn ->
               stream1 = Manager.stream_to(0..9, SimplePipelineToCrash)
               stream2 = Manager.stream_to(10..19, SimplePipelineToCrash)
               stream3 = Manager.stream_to(20..29, SimplePipelineToCrash)

               [result1, result2, result3] =
                 [stream1, stream2, stream3]
                 |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
                 |> Task.await_many()

               state = Manager.__state__(SimplePipelineToCrash)

               [in_progress1, in_progress2, in_progress3] =
                 state.registry_dump
                 |> Map.values()
                 |> Enum.map(& &1.in_progress)

               assert Enum.count(in_progress1) + Enum.count(result1) == 10
               assert Enum.count(in_progress2) + Enum.count(result2) == 10
               assert Enum.count(in_progress3) + Enum.count(result3) == 10
             end) =~ "Last message: {:DOWN, "
    end
  end
end
