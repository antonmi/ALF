defmodule ALF.StopPipelineTest do
  use ExUnit.Case, async: true

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
      SimplePipelineToStop.stop()
    end)
  end

  describe "stop the pipeline" do
    setup do
      SimplePipelineToStop.start()
    end

    test "with several streams" do
      task = run_stop_task(30)

      result =
        0..9
        |> SimplePipelineToStop.stream(timeout: 100)
        |> Enum.to_list()

      errors = Enum.filter(result, fn event -> is_struct(event, ALF.ErrorIP) end)
      assert length(errors) > 0
      assert length(errors) < 10
      Task.await(task)
    end
  end
end
