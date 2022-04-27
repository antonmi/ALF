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
    Unplug,
    Decomposer,
    Recomposer,
    Tbd
  }

  def build(pipe_spec, supervisor_pid, manager_name, pipeline_module, telemetry_enabled \\ nil)
      when is_list(pipe_spec) do
    producer = start_producer(supervisor_pid, manager_name, pipeline_module, telemetry_enabled)

    {last_stages, final_stages} =
      do_build_pipeline(pipe_spec, [producer], supervisor_pid, [], telemetry_enabled)

    consumer =
      start_consumer(
        supervisor_pid,
        last_stages,
        manager_name,
        pipeline_module,
        telemetry_enabled
      )

    {producer, consumer} = set_modules({producer, consumer}, last_stages)
    pipeline = %Pipeline{producer: producer, consumer: consumer, components: final_stages}
    {:ok, pipeline}
  end

  def add_stage_worker(supervisor_pid, [%Stage{} = existing_stage | _] = existing_stages) do
    subscribe_to =
      Enum.map(existing_stage.subscribed_to, fn {pid, _ref} ->
        {pid, [max_demand: 1, cancel: :transient]}
      end)

    stage = %{
      existing_stage
      | subscribe_to: subscribe_to,
        subscribed_to: [],
        number: length(existing_stages),
        count: length(existing_stages) + 1
    }

    {:ok, stage_pid} = DynamicSupervisor.start_child(supervisor_pid, {stage.__struct__, stage})
    %{stage | pid: stage_pid}
  end

  def delete_stage_worker(supervisor_pid, stage) do
    DynamicSupervisor.terminate_child(supervisor_pid, stage.pid)
  end

  defp start_producer(supervisor_pid, manager_name, pipeline_module, telemetry_enabled) do
    producer = %Producer{
      manager_name: manager_name,
      pipeline_module: pipeline_module,
      telemetry_enabled: telemetry_enabled
    }

    {:ok, producer_pid} = DynamicSupervisor.start_child(supervisor_pid, {Producer, producer})
    %{producer | pid: producer_pid}
  end

  defp start_consumer(
         supervisor_pid,
         last_stages,
         manager_name,
         pipeline_module,
         telemetry_enabled
       ) do
    subscribe_to = subscribe_to_opts(last_stages)

    consumer = %Consumer{
      subscribe_to: subscribe_to,
      manager_name: manager_name,
      pipeline_module: pipeline_module,
      telemetry_enabled: telemetry_enabled
    }

    {:ok, consumer_pid} = DynamicSupervisor.start_child(supervisor_pid, {Consumer, consumer})
    %{consumer | pid: consumer_pid}
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

  defp do_build_pipeline(pipe_spec, producers, supervisor_pid, final_stages, telemetry_enabled)
       when is_list(pipe_spec) do
    pipe_spec
    |> Enum.reduce({producers, final_stages}, fn stage_spec, {prev_stages, stages} ->
      case stage_spec do
        %Stage{count: count} = stage ->
          stage_set_ref = make_ref()

          new_stages =
            Enum.map(0..(count - 1), fn number ->
              start_stage(
                %{stage | stage_set_ref: stage_set_ref, number: number},
                supervisor_pid,
                prev_stages,
                telemetry_enabled
              )
            end)

          {new_stages, stages ++ new_stages}

        %Goto{} = goto ->
          goto = start_stage(goto, supervisor_pid, prev_stages, telemetry_enabled)
          {[goto], stages ++ [goto]}

        %DeadEnd{} = dead_end ->
          dead_end = start_stage(dead_end, supervisor_pid, prev_stages, telemetry_enabled)
          {[], stages ++ [dead_end]}

        %GotoPoint{} = goto_point ->
          goto_point = start_stage(goto_point, supervisor_pid, prev_stages, telemetry_enabled)
          {[goto_point], stages ++ [goto_point]}

        %Switch{branches: branches} = switch ->
          switch = start_stage(switch, supervisor_pid, prev_stages, telemetry_enabled)

          {last_stages, branches} =
            Enum.reduce(branches, {[], %{}}, fn {key, inner_pipe_spec},
                                                {all_last_stages, branches} ->
              {last_stages, final_stages} =
                do_build_pipeline(
                  inner_pipe_spec,
                  [{switch, partition: key}],
                  supervisor_pid,
                  [],
                  telemetry_enabled
                )

              {all_last_stages ++ last_stages, Map.put(branches, key, final_stages)}
            end)

          switch = %{switch | branches: branches}

          {last_stages, stages ++ [switch]}

        %Clone{to: pipe_stages} = clone ->
          clone = start_stage(clone, supervisor_pid, prev_stages, telemetry_enabled)

          {last_stages, final_stages} =
            do_build_pipeline(pipe_stages, [clone], supervisor_pid, [], telemetry_enabled)

          clone = %{clone | to: final_stages}

          {last_stages ++ [clone], stages ++ [clone]}

        %Plug{} = plug ->
          plug = start_stage(plug, supervisor_pid, prev_stages, telemetry_enabled)
          {[plug], stages ++ [plug]}

        %Unplug{} = unplug ->
          unplug = start_stage(unplug, supervisor_pid, prev_stages, telemetry_enabled)
          {[unplug], stages ++ [unplug]}

        %Decomposer{} = decomposer ->
          decomposer = start_stage(decomposer, supervisor_pid, prev_stages, telemetry_enabled)
          {[decomposer], stages ++ [decomposer]}

        %Recomposer{} = recomposer ->
          recomposer = start_stage(recomposer, supervisor_pid, prev_stages, telemetry_enabled)
          {[recomposer], stages ++ [recomposer]}

        %Tbd{} = tbd ->
          tbd = start_stage(tbd, supervisor_pid, prev_stages, telemetry_enabled)
          {[tbd], stages ++ [tbd]}
      end
    end)
  end

  defp start_stage(stage, supervisor_pid, prev_stages, telemetry_enabled) do
    stage = %{
      stage
      | subscribe_to: subscribe_to_opts(prev_stages),
        telemetry_enabled: telemetry_enabled
    }

    {:ok, stage_pid} = DynamicSupervisor.start_child(supervisor_pid, {stage.__struct__, stage})
    %{stage | pid: stage_pid}
  end

  defp subscribe_to_opts(stages) do
    Enum.map(stages, fn stage ->
      case stage do
        {stage, partition: key} ->
          {stage.pid, max_demand: 1, cancel: :transient, partition: key}

        stage ->
          {stage.pid, max_demand: 1, cancel: :transient}
      end
    end)
  end
end
