defmodule ALF.Manager.ClientTest do
  use ExUnit.Case, async: false

  alias ALF.{Manager, Manager.Client}

  defmodule Pipeline do
    use ALF.DSL

    @components [
      stage(:foo)
    ]

    def foo(event, _) do
      event + 1
    end
  end

  setup do
    Manager.start(Pipeline)
  end

  test "pipeline works" do
    [result] =
      [1]
      |> Manager.stream_to(Pipeline)
      |> Enum.to_list()

    assert result == 2
  end

  describe "start" do
    test "success" do
      {:ok, pid} = Client.start(Pipeline)
      assert is_pid(pid)
    end

    test "if manager is not run" do
      Manager.stop(Pipeline)

      assert_raise(
        RuntimeError,
        ~r/^The Elixir.ALF.Manager.ClientTest.Pipeline pipeline is not started/,
        fn -> Client.start(Pipeline) end
      )
    end
  end

  describe "call/2" do
    test "call client" do
      {:ok, pid} = Client.start(Pipeline)

      assert Client.call(pid, 1) == 2

      fn ->
        Enum.each(1..100, fn event ->
          assert Client.call(pid, event) == event + 1
        end)
      end
      |> Task.async()
      |> Task.await()
    end
  end

  describe "stop/1" do
    setup do
      {:ok, pid} = Client.start(Pipeline)
      %{pid: pid}
    end

    test "stop the pipeline", %{pid: pid} do
      fn ->
        Enum.each(1..10, fn event ->
          assert Client.call(pid, event) == event + 1
        end)
      end
      |> Task.async()

      Client.stop(pid)
    end
  end
end
