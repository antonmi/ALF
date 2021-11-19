defmodule ALF.DecomposeRecomposeTest do
  use ExUnit.Case, async: false

  alias ALF.{Manager, IP}

  describe "done! in stage" do
    defmodule Pipeline do
      use ALF.DSL

      @components [
        decomposer(:decomposer, function: :decomposer_function),
        recomposer(:recomposer, function: :recomposer_function)
      ]

      def decomposer_function(datum, _opts) do
        String.split(datum)
      end

      def recomposer_function(datum, prev_data, _opts) do
        string = Enum.join(prev_data ++ [datum], " ")

        if String.length(string) > 10 do
          string
        else
          :continue
        end
      end
    end

    setup do
      Manager.start(Pipeline)
    end

    test "returns strings" do
      #        ["foo", "bar", "baz", "anton", "baton", "a"]
      results =
        ["foo foo", "bar bar", "baz baz"]
        |> Manager.stream_to(Pipeline, %{return_ips: true})
        |> Enum.to_list()
        |> IO.inspect()

      Process.sleep(100)
      state = Manager.__state__(Pipeline)
      IO.inspect(state.registry)
      #      assert [
      #               %IP{
      #                 datum: 2,
      #                 history: [{{:add_one, 0}, 1}]
      #               },
      #               %IP{},
      #               %IP{}
      #             ] = results
    end

    #    test "without the return_ips option" do
    #      results =
    #        [1, 2, 3]
    #        |> Manager.stream_to(DoneInStagePipeline)
    #        |> Enum.to_list()
    #
    #      assert results == [2, 3, 4]
    #    end
  end
end
