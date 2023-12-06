defmodule ALF.ComponentThrowTest do
  use ExUnit.Case, async: true

  alias ALF.{ErrorIP, IP}

  describe "throw in stage" do
    defmodule ThrowInStagePipeline do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _) do
        if event == 1 do
          throw("throw in :add_one")
        else
          event + 1
        end
      end

      def mult_two(event, _), do: event * 2
    end

    setup do
      ThrowInStagePipeline.start()
      on_exit(&ThrowInStagePipeline.stop/0)
    end

    test "returns error immediately (skips mult_two)" do
      results =
        [1, 2, 3]
        |> ThrowInStagePipeline.stream(debug: true)
        |> Enum.to_list()

      assert length(Enum.filter(results, &is_struct(&1, IP))) == 2

      %ErrorIP{
        component: %ALF.Components.Stage{function: :add_one},
        ip: %IP{} = ip,
        error: :throw,
        stacktrace: "throw in :add_one"
      } = Enum.find(results, &is_struct(&1, ErrorIP))

      assert [{{:add_one, 0}, _event}] = ip.history
    end
  end

  describe "exit in stage" do
    defmodule ExitInStagePipeline do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _) do
        if event == 1 do
          exit("exit in :add_one")
        else
          event + 1
        end
      end

      def mult_two(event, _), do: event * 2
    end

    setup do
      ExitInStagePipeline.start()
      on_exit(&ExitInStagePipeline.stop/0)
    end

    test "returns error immediately (skips mult_two)" do
      results =
        [1, 2, 3]
        |> ExitInStagePipeline.stream(debug: true)
        |> Enum.to_list()

      assert length(Enum.filter(results, &is_struct(&1, IP))) == 2

      %ErrorIP{
        component: %ALF.Components.Stage{function: :add_one},
        ip: %IP{} = ip,
        error: :exit,
        stacktrace: "exit in :add_one"
      } = Enum.find(results, &is_struct(&1, ErrorIP))

      assert [{{:add_one, 0}, _event}] = ip.history
    end
  end

  describe "throw in switch function" do
    defmodule ThrowInSwitchPipeline do
      use ALF.DSL

      @components [
        switch(:switch_cond,
          branches: %{
            1 => [stage(:add_one)],
            2 => [stage(:mult_two)]
          }
        ),
        stage(:ok)
      ]

      def switch_cond(_event, _) do
        throw("throw in :switch")
      end

      def add_one(event, _) do
        event + 1
      end

      def mult_two(event, _) do
        event * 2
      end

      def ok(event, _), do: event
    end

    setup do
      ThrowInSwitchPipeline.start()
      on_exit(&ThrowInSwitchPipeline.stop/0)
    end

    test "error results" do
      results =
        [1, 2]
        |> ThrowInSwitchPipeline.stream(debug: true)
        |> Enum.to_list()

      assert [
               %ErrorIP{
                 component: %ALF.Components.Switch{name: :switch_cond},
                 ip: %IP{} = ip,
                 error: :throw,
                 stacktrace: "throw in :switch"
               },
               %ErrorIP{}
             ] = results

      assert [{{:switch_cond, 0}, _event}] = ip.history
    end
  end

  describe "error in goto function" do
    defmodule ThrowInGotoPipeline do
      use ALF.DSL

      @components [
        goto_point(:goto_point),
        stage(:add_one),
        goto(:goto_function, to: :goto_point)
      ]

      def goto_function(_event, _) do
        throw("throw in :goto")
      end

      def add_one(event, _), do: event + 1
    end

    setup do
      ThrowInGotoPipeline.start()
      on_exit(&ThrowInGotoPipeline.stop/0)
    end

    test "error results" do
      results =
        [1]
        |> ThrowInGotoPipeline.stream(debug: true)
        |> Enum.to_list()

      Process.sleep(100)

      assert [
               %ErrorIP{
                 component: %ALF.Components.Goto{name: :goto_function},
                 ip: %IP{} = ip,
                 error: :throw,
                 stacktrace: "throw in :goto"
               }
             ] = results

      assert [{{:goto_function, 0}, 2}, {{:add_one, 0}, 1}, {{:goto_point, 0}, 1}] = ip.history
    end
  end
end
