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

    def foo(event, _) do
      Process.sleep(@sleep)
      "#{event}-foo"
    end

    def bar(event, _) do
      Process.sleep(@sleep)
      "#{event}-bar"
    end

    def baz(event, _) do
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
      SimplePipelineToCrash.start()

      on_exit(&SimplePipelineToCrash.stop/0)

      state = Manager.__state__(SimplePipelineToCrash)
      %{state: state}
    end

    test "with one stream", %{state: state} do
      run_kill_task(state, 10)

      assert capture_log(fn ->
               results =
                 0..10
                 |> SimplePipelineToCrash.stream(timeout: 50)
                 |> Enum.to_list()

               errors =
                 results
                 |> Enum.filter(fn event -> is_struct(event, ALF.ErrorIP) end)
                 |> Enum.map(& &1.error)
                 |> Enum.uniq()

               assert errors == [:timeout]
             end) =~ "Last message: {:DOWN, "

      # pipeline is restarted
      assert ["0-foo-bar-baz"] =
               [0]
               |> SimplePipelineToCrash.stream()
               |> Enum.to_list()
    end

    test "with several streams", %{state: state} do
      run_kill_task(state, 30)

      assert capture_log(fn ->
               stream1 = SimplePipelineToCrash.stream(0..9, timeout: 50)
               stream2 = SimplePipelineToCrash.stream(10..19, timeout: 50)
               stream3 = SimplePipelineToCrash.stream(20..29, timeout: 50)

               [result1, result2, result3] =
                 [stream1, stream2, stream3]
                 |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
                 |> Task.await_many()

               errors =
                 (result1 ++ result2 ++ result3)
                 |> Enum.filter(fn event -> is_struct(event, ALF.ErrorIP) end)
                 |> Enum.map(& &1.error)
                 |> Enum.uniq()

               assert errors == [:timeout]
             end) =~ "Last message: {:DOWN, "

      # pipeline is restarted
      assert ["0-foo-bar-baz"] =
               [0]
               |> SimplePipelineToCrash.stream()
               |> Enum.to_list()
    end
  end
end
