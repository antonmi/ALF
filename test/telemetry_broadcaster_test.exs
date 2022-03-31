defmodule ALF.TelemetryBroadcasterTest do
  use ExUnit.Case, async: false

  alias ALF.{Manager, TelemetryBroadcaster}

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(:add_one)
    ]

    def add_one(event, _), do: %{event | number: event.number + 1}
  end

  defmodule TelemetryHandler do
    def handle_event([:alf, :component, :start], _measurements, metadata) do
      agent = metadata.ip[:event][:agent]

      Agent.update(agent, fn list ->
        [{:start, metadata.component, metadata.ip} | list]
      end)
    end

    def handle_event([:alf, :component, :stop], %{duration: _duration}, metadata) do
      agent = metadata.ip[:event][:agent]

      Agent.update(agent, fn list ->
        [{:stop, metadata.component, metadata.ip} | list]
      end)
    end
  end

  setup do
    before = Application.get_env(:alf, :telemetry_enabled)
    Application.put_env(:alf, :telemetry_enabled, true)
    on_exit(fn -> Application.put_env(:alf, :telemetry_enabled, before) end)
    {:ok, agent} = Agent.start_link(fn -> [] end)
    %{agent: agent}
  end

  describe "add and get nodes" do
    test "add node" do
      TelemetryBroadcaster.register_node(:node1@localhost, Mod, :fun)
      assert MapSet.member?(TelemetryBroadcaster.nodes(), {:node1@localhost, Mod, :fun})
    end
  end

  describe "broadcasting" do
    setup do
      Manager.start(SimplePipeline)
      TelemetryBroadcaster.register_node(Node.self(), TelemetryHandler, :handle_event)
      :ok
    end

    test "events", %{agent: agent} do
      [%{agent: agent, number: 1}]
      |> Manager.stream_to(SimplePipeline)
      |> Enum.to_list()

      events =
        agent
        |> Agent.get(& &1)
        |> Enum.reverse()

      assert [
               {:start, %{name: :producer}, %{event: _}},
               {:stop, %{name: :producer}, %{event: _}},
               add_one_event,
               {:stop, %{name: :add_one}, %{event: _}},
               {:start, %{name: :consumer}, %{event: _}},
               {:stop, %{name: :consumer}, %{event: _}}
             ] = events

      assert {
               :start,
               %{
                 name: :add_one,
                 number: 0,
                 pid: _pid,
                 pipeline_module: ALF.TelemetryBroadcasterTest.SimplePipeline
               },
               %{
                 event: %{
                   agent: _agent_pid,
                   number: 1
                 }
               }
             } = add_one_event
    end
  end
end
