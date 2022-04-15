defmodule ALF.ComponentErrorTest do
  use ExUnit.Case, async: false

  alias ALF.{Manager, ErrorIP, IP}

  describe "Error in stage" do
    defmodule ErrorInStagePipeline do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _) do
        if event == 1 do
          raise("Error in :add_one")
        else
          event + 1
        end
      end

      def mult_two(event, _), do: event * 2
    end

    setup do
      Manager.start(ErrorInStagePipeline)
    end

    test "returns error immediately (skips mult_two)" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(ErrorInStagePipeline)
        |> Enum.to_list()

      assert [
               %ErrorIP{
                 component: %ALF.Components.Stage{function: :add_one},
                 ip: %IP{} = ip,
                 error: %RuntimeError{message: "Error in :add_one"}
               },
               6,
               8
             ] = results

      assert [{{:add_one, 0}, _event}] = ip.history
    end
  end

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
      Manager.start(ThrowInStagePipeline)
    end

    test "returns error immediately (skips mult_two)" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(ThrowInStagePipeline)
        |> Enum.to_list()

      assert [
               %ErrorIP{
                 component: %ALF.Components.Stage{function: :add_one},
                 ip: %IP{} = ip,
                 error: :throw,
                 stacktrace: "throw in :add_one"
               },
               6,
               8
             ] = results

      assert [{{:add_one, 0}, _event}] = ip.history
    end
  end

  describe "Error in stage before consumer" do
    defmodule ErrorInStageMultTwoPipeline do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(event, _), do: event + 1

      def mult_two(event, _) do
        if event == 2 do
          raise("Error in :mult_two")
        else
          event
        end
      end
    end

    setup do
      Manager.start(ErrorInStageMultTwoPipeline)
    end

    test "returns error" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(ErrorInStageMultTwoPipeline, %{return_ips: true})
        |> Enum.to_list()

      assert [
               %ErrorIP{
                 component: %ALF.Components.Stage{function: :mult_two},
                 ip: %IP{} = ip,
                 error: %RuntimeError{message: "Error in :mult_two"}
               },
               %IP{},
               %IP{}
             ] = results

      assert [{{:mult_two, 0}, _}, {{:add_one, 0}, _}] = ip.history
    end
  end

  describe "error in switch function" do
    defmodule ErrorInSwitchPipeline do
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
        raise "Error in :switch"
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
      Manager.start(ErrorInSwitchPipeline)
    end

    test "error results" do
      results =
        [1, 2]
        |> Manager.stream_to(ErrorInSwitchPipeline, %{return_ips: true})
        |> Enum.to_list()

      assert [
               %ErrorIP{
                 component: %ALF.Components.Switch{name: :switch_cond},
                 ip: %IP{} = ip,
                 error: %RuntimeError{message: "Error in :switch"}
               },
               %ErrorIP{}
             ] = results

      assert [switch_cond: _event] = ip.history
    end
  end

  describe "error in goto function" do
    defmodule ErrorInGotoPipeline do
      use ALF.DSL

      @components [
        goto_point(:goto_point),
        stage(:add_one),
        goto(:goto_function, to: :goto_point)
      ]

      def goto_function(_event, _) do
        raise "Error in :goto"
      end

      def add_one(event, _), do: event + 1
    end

    setup do
      Manager.start(ErrorInGotoPipeline)
    end

    test "error results" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(ErrorInGotoPipeline)
        |> Enum.to_list()

      assert [
               %ErrorIP{
                 component: %ALF.Components.Goto{name: :goto_function},
                 ip: %IP{} = ip,
                 error: %RuntimeError{message: "Error in :goto"}
               },
               %ErrorIP{},
               %ErrorIP{}
             ] = results

      assert [{:goto_function, _}, {{:add_one, 0}, _}, {:goto_point, _}] = ip.history
    end
  end
end
