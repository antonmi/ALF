defmodule ALF.Builder do
  alias ALF.Pipeline

  alias ALF.Components.{
    Producer,
    Stage,
    Goto,
    DeadEnd,
    GotoPoint,
    Switch,
    Clone,
    Consumer,
    Plug,
    Unplug
  }

  def build(pipe_spec, supervisor_pid) when is_list(pipe_spec) do
    producer = start_producer(supervisor_pid)
    {last_stages, final_stages} = do_build_pipeline(pipe_spec, [producer], supervisor_pid, [])
    consumer = start_consumer(supervisor_pid, last_stages)

    {producer, consumer} = set_modules({producer, consumer}, last_stages)
    pipeline = %Pipeline{producer: producer, consumer: consumer, components: final_stages}
    {:ok, pipeline}
  end

  defp start_producer(supervisor_pid) do
    {:ok, producer_pid} = DynamicSupervisor.start_child(supervisor_pid, {Producer, %{}})
    %Producer{pid: producer_pid}
  end

  defp start_consumer(supervisor_pid, last_stages) do
    subscribe_to = subscribe_to_opts(last_stages)
    opts = %Consumer{subscribe_to: subscribe_to}
    {:ok, consumer_pid} = DynamicSupervisor.start_child(supervisor_pid, {Consumer, opts})
    %Consumer{pid: consumer_pid, subscribe_to: subscribe_to}
  end

  defp set_modules({producer, consumer}, last_stages) do
    last_stage = hd(last_stages)

    producer = %{
      producer
      | pipe_module: last_stage.pipeline_module,
        pipeline_module: last_stage.pipeline_module
    }

    consumer = %{
      consumer
      | pipe_module: last_stage.pipeline_module,
        pipeline_module: last_stage.pipeline_module
    }

    {producer, consumer}
  end

  defp do_build_pipeline(pipe_spec, producers, supervisor_pid, final_stages)
       when is_list(pipe_spec) do
    pipe_spec
    |> Enum.reduce({producers, final_stages}, fn stage_spec, {prev_stages, stages} ->
      case stage_spec do
        %Stage{count: count} = stage ->
          new_stages =
            Enum.map(0..(count - 1), fn number ->
              start_stage(%{stage | number: number}, supervisor_pid, prev_stages)
            end)

          {new_stages, stages ++ new_stages}

        %Goto{} = goto ->
          goto = start_stage(goto, supervisor_pid, prev_stages)
          {[goto], stages ++ [goto]}

        %DeadEnd{} = dead_end ->
          dead_end = start_stage(dead_end, supervisor_pid, prev_stages)
          {[], stages ++ [dead_end]}

        %GotoPoint{} = goto_point ->
          goto_point = start_stage(goto_point, supervisor_pid, prev_stages)
          {[goto_point], stages ++ [goto_point]}

        %Switch{branches: branches} = switch ->
          switch = start_stage(switch, supervisor_pid, prev_stages)

          {last_stages, branches} =
            Enum.reduce(branches, {[], %{}}, fn {key, inner_pipe_spec},
                                                {all_last_stages, branches} ->
              {last_stages, final_stages} =
                do_build_pipeline(inner_pipe_spec, [{switch, partition: key}], supervisor_pid, [])

              {all_last_stages ++ last_stages, Map.put(branches, key, final_stages)}
            end)

          switch = %{switch | branches: branches}

          {last_stages, stages ++ [switch]}

        %Clone{to: pipe_stages} = clone ->
          clone = start_stage(clone, supervisor_pid, prev_stages)

          {last_stages, final_stages} =
            do_build_pipeline(pipe_stages, [clone], supervisor_pid, [])

          clone = %{clone | to: final_stages}

          {last_stages ++ [clone], stages ++ [clone]}

        %Plug{} = plug ->
          plug = start_stage(plug, supervisor_pid, prev_stages)
          {[plug], stages ++ [plug]}

        %Unplug{} = unplug ->
          unplug = start_stage(unplug, supervisor_pid, prev_stages)
          {[unplug], stages ++ [unplug]}
      end
    end)
  end

  defp start_stage(stage, supervisor_pid, prev_stages) do
    stage = %{stage | subscribe_to: subscribe_to_opts(prev_stages)}
    {:ok, stage_pid} = DynamicSupervisor.start_child(supervisor_pid, {stage.__struct__, stage})
    %{stage | pid: stage_pid}
  end

  defp subscribe_to_opts(stages) do
    Enum.map(stages, fn stage ->
      case stage do
        {stage, partition: key} ->
          {stage.pid, max_demand: 1, partition: key}

        stage ->
          {stage.pid, max_demand: 1}
      end
    end)
  end
end
