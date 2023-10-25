defmodule ALF.TelemetryBroadcasterTest do
  use ExUnit.Case, async: false

  alias ALF.{Manager, TelemetryBroadcaster}

  defmodule SimplePipeline do
    use ALF.DSL

    @components [
      stage(:add_one)
    ]

    def add_one(event, _) do
      %{event | number: event.number + 1}
    end
  end

  defmodule TelemetryHandler do
    def handle_event([:alf, :component, :start], _measurements, metadata) do
      agent = metadata.ip[:event][:agent]

      Agent.update(agent, fn list ->
        [{:start, metadata.component, metadata.ip} | list]
      end)

      Process.sleep(5)
    end

    def handle_event([:alf, :component, :stop], %{duration: _duration}, metadata) do
      agent = metadata.ip[:event][:agent]

      Agent.update(agent, fn list ->
        [{:stop, metadata.component, metadata.ip} | list]
      end)

      Process.sleep(5)
    end
  end

  setup do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    %{agent: agent}
  end

  describe "register_remote_function/4" do
    test "register_remote_function" do
      TelemetryBroadcaster.register_remote_function(:node1@localhost, Mod, :fun, interval: 100)

      assert TelemetryBroadcaster.remote_function() ==
               {:node1@localhost, Mod, :fun, interval: 100}
    end
  end

  describe "broadcasting" do
    setup do
      Manager.start(SimplePipeline, telemetry_enabled: true)

      TelemetryBroadcaster.register_remote_function(
        Node.self(),
        TelemetryHandler,
        :handle_event,
        interval: 100
      )

      :ok
    end

    test "events", %{agent: agent} do
      [
        %{agent: agent, number: 1},
        # no events because of interval
        %{agent: agent, number: 2}
      ]
      |> Manager.stream(SimplePipeline)
      |> Enum.to_list()

      Process.sleep(100)

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
