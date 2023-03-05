defmodule ALF.ComponentTelemetryTest do
  use ExUnit.Case
  alias ALF.Manager

  defmodule Pipeline do
    use ALF.DSL

    @components [
      stage(:just_stage)
    ]

    def just_stage(event, _) do
      if event == 2 do
        raise "Ooops!"
      end

      if event == 3 do
        done!(3)
      end

      event + 1
    end
  end

  defmodule Handler do
    def handle_event([:alf, :component, type], measurements, metadata, %{agent: agent}) do
      Agent.update(agent, fn list -> [{type, measurements, metadata} | list] end)
    end
  end

  setup do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    %{agent: agent}
  end

  describe "telemetry events" do
    setup %{agent: agent} do
      Pipeline.start(telemetry_enabled: true)

      :ok =
        :telemetry.attach_many(
          "test-events-handler",
          [
            [:alf, :component, :start],
            [:alf, :component, :stop],
            [:alf, :component, :exception]
          ],
          &Handler.handle_event/4,
          %{agent: agent}
        )

      on_exit(fn ->
        :telemetry.detach("test-events-handler")
        Pipeline.stop()
      end)
    end

    test "check telemetry events", %{agent: agent} do
      [result] =
        [1]
        |> Pipeline.stream()
        |> Enum.to_list()

      Process.sleep(5)
      assert result == 2

      [consumer_stop, consumer_start, stage_stop, stage_start, producer_stop, producer_start] =
        Agent.get(agent, & &1)

      check_producer_events(producer_stop, producer_start)
      check_stage_events(stage_stop, stage_start)
      check_consumer_events(consumer_stop, consumer_start)
    end

    def check_stage_events(stage_stop, stage_start) do
      assert {
               :stop,
               %{duration: _duration},
               %{
                 component: %{
                   name: :just_stage,
                   number: 0,
                   pipeline_module: __MODULE__.Pipeline
                 },
                 ip: %{event: 2, type: :ip},
                 telemetry_span_context: _ref
               }
             } = stage_stop

      assert {
               :start,
               %{system_time: _system_time},
               %{
                 component: _component,
                 ip: %{event: 1, type: :ip},
                 telemetry_span_context: _ref
               }
             } = stage_start
    end

    def check_producer_events(producer_stop, producer_start) do
      assert {
               :stop,
               %{duration: _duration},
               %{
                 component: %{
                   name: :producer,
                   pipeline_module: __MODULE__.Pipeline
                 },
                 ip: %{event: 1, type: :ip},
                 telemetry_span_context: _ref
               }
             } = producer_stop

      assert {
               :start,
               %{system_time: _system_time},
               %{
                 component: %{
                   name: :producer,
                   pipeline_module: __MODULE__.Pipeline
                 },
                 ip: %{event: 1, type: :ip},
                 telemetry_span_context: _ref
               }
             } = producer_start
    end

    def check_consumer_events(consumer_stop, consumer_start) do
      assert {
               :stop,
               %{duration: _duration},
               %{
                 component: %{name: :consumer, pipeline_module: __MODULE__.Pipeline},
                 ip: %{event: 2, type: :ip},
                 telemetry_span_context: _ref
               }
             } = consumer_stop

      assert {
               :start,
               %{system_time: _system_time},
               %{
                 component: %{name: :consumer, pipeline_module: __MODULE__.Pipeline},
                 ip: %{event: 2, type: :ip},
                 telemetry_span_context: _ref
               }
             } = consumer_start
    end

    test "check events if error happens", %{agent: agent} do
      [result] =
        [2]
        |> Pipeline.stream()
        |> Enum.to_list()

      assert %ALF.ErrorIP{} = result

      [stage_stop, _, _, _] = Agent.get(agent, & &1)
      {:stop, _, %{ip: ip}} = stage_stop
      assert ip[:error] == %RuntimeError{message: "Ooops!"}
      assert ip[:stacktrace]
      assert ip[:type] == :error_ip
    end

    test "done! event", %{agent: agent} do
      [result] =
        [3]
        |> Pipeline.stream()
        |> Enum.to_list()

      assert result == 3
      Process.sleep(5)

      [stage_stop, _, _, _] = Agent.get(agent, & &1)
      {:stop, _, %{ip: ip}} = stage_stop
      assert ip[:type] == :ip
      assert ip[:done!] == true
    end
  end

  describe "telemetry events for sync case" do
    setup %{agent: agent} do
      Pipeline.start(telemetry_enabled: true, sync: true)

      :ok =
        :telemetry.attach_many(
          "test-events-handler",
          [
            [:alf, :component, :start],
            [:alf, :component, :stop],
            [:alf, :component, :exception]
          ],
          &Handler.handle_event/4,
          %{agent: agent}
        )

      on_exit(fn ->
        :telemetry.detach("test-events-handler")
        Pipeline.stop()
      end)
    end

    test "check telemetry events", %{agent: agent} do
      [result] =
        [1]
        |> Pipeline.stream()
        |> Enum.to_list()

      assert result == 2

      [consumer_stop, consumer_start, stage_stop, stage_start, producer_stop, producer_start] =
        Agent.get(agent, & &1)

      check_producer_events(producer_stop, producer_start)
      check_stage_events(stage_stop, stage_start)
      check_consumer_events(consumer_stop, consumer_start)
    end

    test "check events if error happens", %{agent: agent} do
      [result] =
        [2]
        |> Pipeline.stream()
        |> Enum.to_list()

      assert %ALF.ErrorIP{} = result

      [stage_stop, _, _, _] = Agent.get(agent, & &1)
      {:stop, _, %{ip: ip}} = stage_stop
      assert ip[:error] == %RuntimeError{message: "Ooops!"}
      assert ip[:stacktrace]
      assert ip[:type] == :error_ip
    end

    test "done! event", %{agent: agent} do
      [result] =
        [3]
        |> Manager.stream_to(Pipeline)
        |> Enum.to_list()

      assert result == 3

      [stage_stop, _, _, _] = Agent.get(agent, & &1)
      {:stop, _, %{ip: ip}} = stage_stop
      assert ip[:type] == :ip
      assert ip[:done!] == true
    end
  end
end
