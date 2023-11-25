defmodule ALF.Examples.DistrTrans.TwoPhaseCommitSeq.TPCPipeline do
  use ALF.DSL

  defstruct data: 0,
            first_ready: false,
            second_ready: false,
            both_ready: false,
            done: false

  @components [
    stage(:first_ready),
    stage(:second_ready),
    stage(:both_ready),
    switch(:if_both_ready,
      branches: %{
        yes: [stage(:first_commit), stage(:second_commit), stage(:set_done)],
        no: [stage(:first_abort), stage(:second_abort)]
      }
    )
  ]

  def first_ready(event, _) do
    if event.data == :first_fail do
      %{event | data: 0, first_ready: false}
    else
      %{event | data: event.data + 1, first_ready: true}
    end
  end

  def second_ready(event, _) do
    if event.data > 10 do
      %{event | data: event.data, second_ready: false}
    else
      %{event | data: event.data + 2, second_ready: true}
    end
  end

  def both_ready(event, _) do
    if event.first_ready and event.second_ready do
      %{event | both_ready: true}
    else
      %{event | both_ready: false}
    end
  end

  def if_both_ready(event, _) do
    if event.both_ready, do: :yes, else: :no
  end

  def first_commit(event, _), do: event

  def second_commit(event, _), do: event

  def set_done(event, _), do: %{event | done: true}

  def first_abort(event, _), do: %{event | data: event.data - 1}

  def second_abort(event, _), do: %{event | data: event.data - 2}
end

defmodule ALF.Examples.DistrTrans.TwoPhaseCommitSeq.TPCPipelineWithPlug do
  use ALF.DSL
  alias ALF.Examples.DistrTrans.TwoPhaseCommitSeq.TPCPipeline

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

defmodule ALF.Examples.DistrTrans.TwoPhaseCommitSeqTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.DistrTrans.TwoPhaseCommitSeq.TPCPipeline

  setup do
    TPCPipeline.start()
    on_exit(&TPCPipeline.stop/0)
  end

  test "success" do
    result = TPCPipeline.call(%TPCPipeline{data: 1})
    assert result.done
    assert result.data == 4
  end

  test "first_fail" do
    result = TPCPipeline.call(%TPCPipeline{data: :first_fail})
    refute result.done
    assert result.data == -1
  end

  test "second_fail" do
    result = TPCPipeline.call(%TPCPipeline{data: 10})
    refute result.done
    assert result.data == 8
  end
end

defmodule ALF.Examples.DistrTrans.TwoPhaseCommitSeqWithPlugTest do
  use ExUnit.Case, async: true

  alias ALF.Examples.DistrTrans.TwoPhaseCommitSeq.TPCPipelineWithPlug

  setup do
    TPCPipelineWithPlug.start()
    on_exit(&TPCPipelineWithPlug.stop/0)
  end

  test "success" do
    result = TPCPipelineWithPlug.call(%TPCPipelineWithPlug{data: 1})
    assert result.success
    assert result.data == "4"
  end

  test "first_fail" do
    result = TPCPipelineWithPlug.call(%TPCPipelineWithPlug{data: :first_fail})
    refute result.success
    assert result.data == :nope
  end

  test "second_fail" do
    result = TPCPipelineWithPlug.call(%TPCPipelineWithPlug{data: 10})
    refute result.success
    assert result.data == :nope
  end
end
