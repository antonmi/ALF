defmodule ALF.SyncRun.ErrorAndDone.Pipeline do
  use ALF.DSL

  @components [
    stage(:add_one),
    done(:is_enough),
    stage(:mult_by_two),
    stage(:minus_three)
  ]

  def add_one(event, _), do: event + 1
  def is_enough(event, _), do: event == 3
  def mult_by_two(event, _), do: if(event == 4, do: raise("error"), else: event * 2)
  def minus_three(event, _), do: event - 3
end

defmodule ALF.SyncRun.ErrorAndDoneTest do
  use ExUnit.Case, async: true

  alias ALF.SyncRun.ErrorAndDone.Pipeline

  setup do
    Pipeline.start(sync: true)
    on_exit(&Pipeline.stop/0)
  end

  test "sync run, done" do
    results =
      [2]
      |> Pipeline.stream()
      |> Enum.to_list()

    assert [3] = results
  end

  test "sync run, raise error" do
    results =
      [3]
      |> Pipeline.stream()
      |> Enum.to_list()

    assert [error_ip] = results
    assert error_ip.error == %RuntimeError{message: "error"}
  end
end
