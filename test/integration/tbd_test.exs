defmodule ALF.TbdTest do
  use ExUnit.Case, async: false

  alias ALF.Manager

  defmodule TbdPipeline do
    use ALF.DSL

    @components [
      tbd(),
      tbd(:placeholder)
    ]
  end

  setup do
    Manager.start(TbdPipeline)
  end

  test "returns the event" do
    [result] =
      [1]
      |> Manager.stream_to(TbdPipeline, return_ips: true)
      |> Enum.to_list()

    assert result.event == 1
    assert result.history == [{:placeholder, 1}, {:tbd, 1}]
  end
end
