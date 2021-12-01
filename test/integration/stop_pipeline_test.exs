defmodule ALF.StopPipelineTest do
  use ExUnit.Case, async: false

  alias ALF.Manager

  defmodule SimplePipelineToStop do
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

  def run_stop_task(milliseconds) do
    Task.async(fn ->
      Process.sleep(milliseconds)
      Manager.stop(SimplePipelineToStop)
    end)
  end

  describe "stop the pipeline" do
    setup do
      Manager.start(SimplePipelineToStop)
    end

    test "with several streams" do
      task = run_stop_task(30)

      stream = Manager.stream_to(0..9, SimplePipelineToStop)

      [result] =
        [stream]
        |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
        |> Task.await_many()

      assert Enum.count(result) < 10

      Task.await(task)
    end
  end
end
