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

    def foo(datum, _opts) do
      Process.sleep(@sleep)
      "#{datum}-foo"
    end

    def bar(datum, _opts) do
      Process.sleep(@sleep)
      "#{datum}-bar"
    end

    def baz(datum, _opts) do
      Process.sleep(@sleep)
      "#{datum}-baz"
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
        Process.sleep(50)
      end)

      state = Manager.__state__(SimplePipelineToCrash)
      %{state: state}
    end

    test "with one stream", %{state: state} do
      run_kill_task(state, 10)

      assert capture_log(fn ->
               result =
                 0..9
                 |> Manager.stream_to(SimplePipelineToCrash)
                 |> Enum.to_list()

               assert Enum.sort(result) == Enum.map(0..9, &"#{&1}-foo-bar-baz")
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

               Process.sleep(1000)

               assert Enum.sort(result1) == Enum.map(0..9, &"#{&1}-foo-bar-baz")
               assert Enum.sort(result2) == Enum.map(10..19, &"#{&1}-foo-bar-baz")
               assert Enum.sort(result3) == Enum.map(20..29, &"#{&1}-foo-bar-baz")
             end) =~ "Last message: {:DOWN, "
    end
  end
end
