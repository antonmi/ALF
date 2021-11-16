defmodule ALF.CrashPipelineTest do
  use ExUnit.Case, async: true

  alias ALF.Manager

  defmodule SimplePipelineToStop do
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

  def run_stop_task(milliseconds) do
    Task.async(fn ->
      Process.sleep(milliseconds)
      Manager.stop(SimplePipelineToStop)
    end)
  end

  describe "kill stage" do
    setup do
      Manager.start(SimplePipelineToStop)
    end

    test "with several streams" do
      run_stop_task(30)

      stream = Manager.stream_to(0..9, SimplePipelineToStop)

      [result] =
        [stream]
        |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
        |> Task.await_many()

      assert Enum.count(result) < 10
    end
  end
end