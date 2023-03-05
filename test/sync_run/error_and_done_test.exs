defmodule ALF.SyncRun.ErrorAndDone.Pipeline do
  use ALF.DSL

  @components [
    stage(:add_one),
    stage(:mult_by_two),
    stage(:minus_three)
  ]

  def add_one(event, _), do: if(event == 3, do: done!(event), else: event + 1)
  def mult_by_two(event, _), do: if(event == 3, do: raise("error"), else: event * 2)
  def minus_three(event, _), do: event - 3
end

defmodule ALF.SyncRun.ErrorAndDoneTest do
  use ExUnit.Case

  alias ALF.SyncRun.ErrorAndDone.Pipeline

  setup do
    Pipeline.start(sync: true)
    on_exit(fn -> Pipeline.stop() end)
  end

  test "sync run" do
    results =
      [1, 2, 3]
      |> Pipeline.stream()
      |> Enum.to_list()

    assert [1, error_ip, 3] = results
    assert error_ip.error == %RuntimeError{message: "error"}
  end
end
