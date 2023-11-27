defmodule ALF.BroadcasterTest do
  use ExUnit.Case, async: true

  defmodule Pipeline do
    use ALF.DSL

    @components [
      stage(:add_one),
      broadcaster(:the_broadcaster),
      stage(:mult_two, count: 3)
    ]

    def add_one(event, _), do: event + 1
    def mult_two(event, _), do: event * 2
  end

  describe "async pipeline" do
    setup do
      Pipeline.start()
      on_exit(&Pipeline.stop/0)
    end

    test "it works" do
      assert Pipeline.call(1) == [4, 4, 4]
      ips = Pipeline.call(1, debug: true)
      assert Enum.map(ips, & &1.composed) == [true, true, true]
    end
  end

  describe "InfoPipeline" do
    defmodule InfoPipeline do
      use ALF.DSL

      @components [
        switch(:route,
          branches: [
            data: [tbd(), stage(:add_one, count: 2)],
            info: [tbd(), broadcaster(:the_broadcaster, count: 2)]
          ]
        ),
        stage(:mult_two, count: 3)
      ]

      def route({:info, _}, _), do: :info
      def route(_event, _), do: :data

      def add_one(event, _), do: event + 1

      def mult_two({:info, info}, _), do: info
      def mult_two(event, _), do: event * 2
    end

    setup do
      InfoPipeline.start()
      on_exit(&InfoPipeline.stop/0)
    end

    test "it works" do
      assert InfoPipeline.call(1) == 4
      assert InfoPipeline.call({:info, :info}) == [:info, :info, :info]
    end
  end
end
