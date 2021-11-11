defmodule ALF.ComponentErrorTest do
  use ExUnit.Case, async: true

  alias ALF.{Manager, ErrorIP, IP}

  describe "Error in stage" do
    defmodule ErrorInStagePipeline do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(_datum, _opts), do: raise "Error in :add_one"
      def mult_two(datum, _opts), do: datum * 2
    end

    setup do
      Manager.start(ErrorInStagePipeline)
    end

    test "returns error immediately (skips mult_two)" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(ErrorInStagePipeline, %{return_ips: true})
        |> Enum.to_list()

      assert [
        %ErrorIP{
          component: %ALF.Components.Stage{function: :add_one},
          ip: %IP{} = ip,
          error: %RuntimeError{message: "Error in :add_one"}
        },
        %ErrorIP{},
        %ErrorIP{},
      ] = results

      assert [{{:add_one, 0}, _datum}] = ip.history
    end
  end

  describe "Error in stage before consumer" do
    defmodule ErrorInStageMultTwoPipeline do
      use ALF.DSL

      @components [
        stage(:add_one),
        stage(:mult_two)
      ]

      def add_one(datum, _opts), do: datum + 1
      def mult_two(_datum, _opts), do: raise "Error in :mult_two"
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
               %ErrorIP{},
               %ErrorIP{},
             ] = results

      assert [{{:mult_two, 0}, _}, {{:add_one, 0}, _}] = ip.history
    end
  end

  describe "error in switch function" do
    defmodule ErrorInSwitchPipeline do
      use ALF.DSL

      @components [
        switch(:switch,
          partitions: %{
            1 => [stage(:add_one)],
            2 => [stage(:mult_two)]
          },
          cond: :switch_cond
        ),
        stage(:ok)
      ]

      def switch_cond(_datum, _opts) do
        raise "Error in :switch"
      end

      def add_one(_datum, _opts), do: raise "Error in :add_one"
      def mult_two(datum, _opts), do: datum * 2
      def ok(datum, _opts), do: datum
    end

    setup do
      Manager.start(ErrorInSwitchPipeline)
    end

    test "error results" do
      results =
        [1, 2, 3]
        |> Manager.stream_to(ErrorInSwitchPipeline, %{return_ips: true})
        |> Enum.to_list()

      assert [
               %ErrorIP{
                 component: %ALF.Components.Switch{name: :switch},
                 ip: %IP{} = ip,
                 error: %RuntimeError{message: "Error in :switch"}
               },
               %ErrorIP{},
               %ErrorIP{},
             ] = results

      assert [switch: _datum] = ip.history
    end
  end

  describe "error in goto function" do
    defmodule ErrorInGotoPipeline do
      use ALF.DSL

      @components [
        goto_point(:goto_point),
        stage(:add_one),
        goto(:goto, to: :goto_point, if: :if_func)
      ]

      def if_func(_datum, _opts) do
        raise "Error in :goto"
      end

      def add_one(datum, _opts), do: datum + 1
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
                 component: %ALF.Components.Goto{name: :goto},
                 ip: %IP{} = ip,
                 error: %RuntimeError{message: "Error in :goto"}
               },
               %ErrorIP{},
               %ErrorIP{},
             ] = results


      assert [{:goto, _}, {{:add_one, 0}, _}, {:goto_point, _}] = ip.history
    end
  end
end
