defmodule ALF.Examples.DistrTrans.TwoPhaseCommitParallel.TPCPipeline do
  use ALF.DSL

  defstruct data: 0,
            route_to: nil,
            ready: false,
            both_ready: false,
            done: false

  @components [
    composer(:decompose),
    switch(:route_event,
      branches: %{
        first: [stage(:first_ready)],
        second: [stage(:second_ready)]
      }
    ),
    composer(:both_ready),
    switch(:if_both_ready,
      branches: %{
        true: [
          composer(:decompose),
          switch(:route_event,
            branches: %{
              first: [stage(:first_commit)],
              second: [stage(:second_commit)]
            }
          ),
          composer(:both_committed)
        ],
        false: [
          composer(:decompose),
          switch(:route_event,
            branches: %{
              first: [stage(:first_abort)],
              second: [stage(:second_abort)]
            }
          ),
          composer(:both_aborted)
        ]
      }
    )
  ]

  def decompose(event, _memo, _) do
    {[%{event | route_to: :first}, %{event | route_to: :second}], nil}
  end

  def route_event(event, _), do: event.route_to

  def first_ready(event, _) do
    Process.sleep(1)

    if event.data == :first_fail do
      %{event | data: 0, ready: false}
    else
      %{event | data: event.data + 1, ready: true}
    end
  end

  def second_ready(event, _) do
    if event.data > 10 do
      %{event | data: event.data, ready: false}
    else
      %{event | data: event.data + 2, ready: true}
    end
  end

  def both_ready(event, nil, _), do: {[], event}

  def both_ready(event, prev_event, _) do
    if event.ready and prev_event.ready do
      {[%{event | both_ready: true, data: event.data + prev_event.data}], nil}
    else
      {[%{event | both_ready: false}], nil}
    end
  end

  def if_both_ready(event, _), do: event.both_ready

  def first_commit(event, _) do
    Process.sleep(1)
    event
  end

  def second_commit(event, _), do: event

  def both_committed(event, nil, _), do: {[], event}

  def both_committed(event, _prev_event, _) do
    {[%{event | done: true}], nil}
  end

  def first_abort(event, _), do: %{event | data: event.data - 1}
  def second_abort(event, _), do: %{event | data: event.data - 2}

  def both_aborted(event, nil, _), do: {[], event}

  def both_aborted(event, _prev_event, _) do
    {[%{event | done: false}], nil}
  end
end

defmodule ALF.Examples.DistrTrans.TwoPhaseCommitParallel.TPCPipelineWithPlug do
  use ALF.DSL
  alias ALF.Examples.DistrTrans.TwoPhaseCommitParallel.TPCPipeline

  defstruct data: 0, success: false

  defmodule Adapter do
    def plug(event, _opts) do
      %TPCPipeline{data: event.data}
    end

    def unplug(event, prev_event, _opts) do
      if event.done do
        %{prev_event | data: event.data, success: true}
      else
        %{prev_event | data: :nope, success: false}
      end
    end
  end

  @components [
    stage(:do_nothing),
    plug_with(Adapter) do
      from(TPCPipeline)
    end,
    done(:if_error),
    stage(:number_to_string)
  ]

  def do_nothing(event, _), do: event
  def if_error(event, _), do: !event.success
  def number_to_string(event, _), do: %{event | data: "#{event.data}"}
end

defmodule ALF.Examples.DistrTrans.TwoPhaseCommitParallelTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.DistrTrans.TwoPhaseCommitParallel.TPCPipeline

  setup do
    TPCPipeline.start()
    on_exit(&TPCPipeline.stop/0)
  end

  test "success" do
    result = TPCPipeline.call(%TPCPipeline{data: 1})
    assert result.done
    assert result.data == 5
  end

  test "first_fail" do
    result = TPCPipeline.call(%TPCPipeline{data: :first_fail})
    refute result.done
  end

  test "second_fail" do
    result = TPCPipeline.call(%TPCPipeline{data: 11})
    refute result.done
  end
end

defmodule ALF.Examples.DistrTrans.TwoPhaseCommitParallelWithPlugTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.DistrTrans.TwoPhaseCommitParallel.TPCPipelineWithPlug

  setup do
    TPCPipelineWithPlug.start()
    on_exit(&TPCPipelineWithPlug.stop/0)
  end

  test "success" do
    result = TPCPipelineWithPlug.call(%TPCPipelineWithPlug{data: 1})
    assert result.success
    assert result.data == "5"
  end

  test "first_fail" do
    result = TPCPipelineWithPlug.call(%TPCPipelineWithPlug{data: :first_fail})
    refute result.success
    assert result.data == :nope
  end

  test "second_fail" do
    result = TPCPipelineWithPlug.call(%TPCPipelineWithPlug{data: 11})
    refute result.success
    assert result.data == :nope
  end
end
