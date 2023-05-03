defmodule ALF.TbdTest do
  use ExUnit.Case, async: true

  defmodule TbdPipeline do
    use ALF.DSL

    @components [
      tbd(),
      tbd(:placeholder)
    ]
  end

  setup do
    TbdPipeline.start()
    on_exit(&TbdPipeline.stop/0)
  end

  test "returns the event" do
    [result] =
      [1]
      |> TbdPipeline.stream(return_ips: true)
      |> Enum.to_list()

    assert result.event == 1
    assert result.history == [{:placeholder, 1}, {:tbd, 1}]
  end
end
