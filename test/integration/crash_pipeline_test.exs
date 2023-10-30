defmodule ALF.CrashPipelineTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  defmodule SimplePipelineToCrash do
    use ALF.DSL

    @sleep 1

    @components [
      stage(:foo),
      stage(:bar, count: 2),
      stage(:baz, count: 2)
    ]

    def foo(event, _) do
      Process.sleep(@sleep)

      "#{event}-foo"
    end

    def bar(event, _) do
      Process.sleep(@sleep)
      if String.starts_with?(event, "6"), do: Process.exit(self(), :kill)

      "#{event}-bar"
    end

    def baz(event, _) do
      Process.sleep(@sleep)
      if String.starts_with?(event, "7"), do: Process.exit(self(), :kill)

      "#{event}-baz"
    end
  end

  describe "crashes" do
    setup do
      SimplePipelineToCrash.start()
      Process.sleep(10)
      on_exit(&SimplePipelineToCrash.stop/0)
      %{state: ALF.Manager.__state__(SimplePipelineToCrash)}
    end

    test "with one stream two crashes", %{state: state} do
      capture_log(fn ->
        results =
          0..9
          |> SimplePipelineToCrash.stream(timeout: 20)
          |> Enum.to_list()

        assert length(results) == 10
        errors = Enum.filter(results, fn event -> is_struct(event, ALF.ErrorIP) end)
        assert Enum.uniq(Enum.map(errors, & &1.error)) == [:timeout]
        Process.sleep(10)
      end)

      pids = Enum.map(Map.values(state.stages), & &1.pid)
      new_state = ALF.Manager.__state__(SimplePipelineToCrash)
      new_pids = Enum.map(Map.values(new_state.stages), & &1.pid)

      assert new_state.pipeline_sup_pid == state.pipeline_sup_pid
      assert length(pids -- new_pids) >= 2
    end

    test "with one stream lost of crashes (pipeline supervisor crash)", %{state: state} do
      capture_log(fn ->
        results =
          [1, 6, 7, 1, 6, 2, 7, 6, 7, 2]
          |> SimplePipelineToCrash.stream(timeout: 20)
          |> Enum.to_list()

        assert length(results) == 10

        errors = Enum.filter(results, fn event -> is_struct(event, ALF.ErrorIP) end)
        assert length(errors) >= 6
        Process.sleep(20)
      end) =~ "terminating"

      pids = Enum.map(Map.values(state.stages), & &1.pid)
      new_state = ALF.Manager.__state__(SimplePipelineToCrash)
      new_pids = Enum.map(Map.values(new_state.stages), & &1.pid)

      assert length(pids -- new_pids) == 7
      assert new_state.pipeline_sup_pid != state.pipeline_sup_pid

      assert SimplePipelineToCrash.call(1) == "1-foo-bar-baz"
    end

    test "with several streams" do
      capture_log(fn ->
        stream1 = SimplePipelineToCrash.stream(0..9, timeout: 50)
        stream2 = SimplePipelineToCrash.stream(10..19, timeout: 50)
        stream3 = SimplePipelineToCrash.stream(20..29, timeout: 50)

        [result1, result2, result3] =
          [stream1, stream2, stream3]
          |> Enum.map(&Task.async(fn -> Enum.to_list(&1) end))
          |> Task.await_many()

        assert length(result1) == 10
        assert length(result2) == 10
        assert length(result3) == 10

        errors =
          Enum.filter(result1 ++ result2 ++ result3, fn event -> is_struct(event, ALF.ErrorIP) end)

        assert length(errors) >= 2
        Process.sleep(10)
      end)

      assert SimplePipelineToCrash.call(1) == "1-foo-bar-baz"
    end
  end

  def kill(pid) do
    capture_log(fn ->
      Process.exit(pid, :kill)
      Process.sleep(20)
    end) =~ "is :DOWN with reason: killed"
  end

  describe "crashes in different components" do
    defmodule BubbleSortWithSwitchPipeline do
      use ALF.DSL

      defstruct [:list, :new_list, :max, :ready]

      @components [
        stage(:build_struct),
        goto_point(:goto_point),
        stage(:find_max),
        tbd(:tbd),
        stage(:update_new_list, count: 2),
        stage(:rebuild_list, count: 2),
        clone(:logging, to: [stage(:report_step), dead_end(:after_report)]),
        switch(:ready_or_not,
          branches: %{
            ready: [stage(:format_output)],
            not_ready: [goto(true, to: :goto_point, name: :just_go)]
          }
        )
      ]

      def build_struct(list, _) do
        %__MODULE__{list: list, new_list: [], max: 0, ready: false}
      end

      def find_max(struct, _) do
        %{struct | max: Enum.max(struct.list)}
      end

      def update_new_list(struct, _) do
        %{struct | new_list: [struct.max | struct.new_list]}
      end

      def rebuild_list(struct, _) do
        %{struct | list: struct.list -- [struct.max]}
      end

      def report_step(struct, _) do
        # IO.inspect("Step: #{inspect struct}", charlists: :as_lists)
        struct
      end

      def format_output(struct, _) do
        struct.new_list
      end

      def ready_or_not(struct, _) do
        if Enum.empty?(struct.list) do
          :ready
        else
          :not_ready
        end
      end
    end

    setup do
      BubbleSortWithSwitchPipeline.start()
      Process.sleep(10)

      on_exit(&BubbleSortWithSwitchPipeline.stop/0)
      %{components: BubbleSortWithSwitchPipeline.components()}
    end

    test "crash in producer", %{components: components} do
      producer = Enum.find(components, &(&1.name == :producer))
      kill(producer.pid)
      assert BubbleSortWithSwitchPipeline.call([3, 1, 2]) == [1, 2, 3]
    end

    test "crash in consumer", %{components: components} do
      consumer = Enum.find(components, &(&1.name == :consumer))
      kill(consumer.pid)
      assert BubbleSortWithSwitchPipeline.call([3, 1, 2]) == [1, 2, 3]
    end

    test "crash in goto_point", %{components: components} do
      goto_point = Enum.find(components, &(&1.name == :goto_point))
      kill(goto_point.pid)
      assert BubbleSortWithSwitchPipeline.call([3, 1, 2]) == [1, 2, 3]
    end

    test "crash in goto", %{components: components} do
      goto = Enum.find(components, &(&1.name == :just_go))
      kill(goto.pid)
      assert BubbleSortWithSwitchPipeline.call([3, 1, 2]) == [1, 2, 3]
    end

    test "crash in dead_end", %{components: components} do
      dead_end = Enum.find(components, &(&1.name == :after_report))
      kill(dead_end.pid)
      assert BubbleSortWithSwitchPipeline.call([3, 1, 2]) == [1, 2, 3]
    end

    test "crash in clone", %{components: components} do
      clone = Enum.find(components, &(&1.name == :logging))
      kill(clone.pid)
      assert BubbleSortWithSwitchPipeline.call([3, 1, 2]) == [1, 2, 3]
    end

    test "crash in switch", %{components: components} do
      switch = Enum.find(components, &(&1.name == :ready_or_not))
      kill(switch.pid)
      Process.sleep(50)
      assert BubbleSortWithSwitchPipeline.call([3, 1, 2]) == [1, 2, 3]
    end

    test "crash in tbd", %{components: components} do
      tbd = Enum.find(components, &(&1.name == :tbd))
      kill(tbd.pid)
      assert BubbleSortWithSwitchPipeline.call([3, 1, 2]) == [1, 2, 3]
    end
  end

  describe "crashes in decomposer and recomposer" do
    defmodule DeRePipeline do
      use ALF.DSL

      @components [
        decomposer(:decomposer_function),
        recomposer(:recomposer_function)
      ]

      def decomposer_function(event, _) do
        String.split(event)
      end

      def recomposer_function(event, prev_events, _) do
        string = Enum.join(prev_events ++ [event], " ")

        if String.length(string) > 10 do
          string
        else
          # testing identical behaviour
          if Enum.random([true, false]) do
            :continue
          else
            {nil, prev_events ++ [event]}
          end
        end
      end
    end

    setup do
      DeRePipeline.start()
      Process.sleep(10)
      on_exit(&DeRePipeline.stop/0)
      %{components: DeRePipeline.components()}
    end

    def it_works! do
      [ip1, ip2] =
        ["foo foo", "bar bar", "baz baz"]
        |> DeRePipeline.stream(debug: true)
        |> Enum.to_list()

      assert ip1.event == "foo foo bar"
      assert ip2.event == "bar baz baz"
    end

    test "returns strings" do
      it_works!()
    end

    test "kill decomposer", %{components: components} do
      decomposer = Enum.find(components, &(&1.name == :decomposer_function))
      kill(decomposer.pid)
      Process.sleep(50)
      it_works!()
    end

    test "kill recomposer", %{components: components} do
      recomposer = Enum.find(components, &(&1.name == :recomposer_function))
      kill(recomposer.pid)
      Process.sleep(10)
      it_works!()
    end
  end
end
