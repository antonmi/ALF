defmodule ALF.SyncRunner do
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

  alias ALF.Builder
  alias ALF.{ErrorIP, IP}
  alias ALF.Manager.Streamer
  alias ALF.Pipeline

  def stream_to(stream, pipeline) do
    # TODO no need in stream_ref for sync case
    stream_ref = make_ref()
    pipeline = Builder.build_sync(pipeline.alf_components(), pipeline, true)
    transform_stream(stream, pipeline, stream_ref)
  end

  def transform_stream(stream, pipeline, stream_ref) do
    stream
    |> Stream.chunk_while(
      [],
      fn event, acc ->
        [ip] = Streamer.build_ips([event], stream_ref, pipeline)
        ips = run(pipeline, ip)
        acc = acc ++ Streamer.format_output(ips, false, false)
        {:cont, acc, []}
      end,
      fn [] ->
        {:cont, [], []}
      end
    )
    |> Stream.flat_map(& &1)
  end

  def run([first | _] = pipeline, %IP{sync_path: nil} = ip) do
    {path, true} = path(pipeline, first.pid)
    ip = %{ip | sync_path: path}
    run(pipeline, ip)
  end

  def run([first | _] = pipeline, ip) do
    do_run(pipeline, ip, [], [])
  end

  defp do_run(pipeline, %IP{sync_path: sync_path} = ip, queue, results) do
    case sync_path do
      [] ->
        do_run(pipeline, nil, queue, [ip | results])

      [cref | rest] ->
        ip = %{ip | sync_path: rest}
        component = find_component(pipeline, cref)

        case run_component(component, ip, pipeline) do
          nil ->
            do_run(pipeline, nil, queue, results)

          [ip | ips] ->
            do_run(pipeline, ip, queue ++ ips, results)

          ip ->
            do_run(pipeline, ip, queue, results)
        end
    end
  end

  defp do_run(pipeline, nil, [], results), do: Enum.reverse(results)

  defp do_run(pipeline, nil, [ip | ips], results) do
    do_run(pipeline, ip, ips, results)
  end

  defp do_run(pipeline, %ErrorIP{} = error_ip, queue, results) do
    Enum.reverse([error_ip | results])
  end

  #  defp do_run(pipeline, %IP{done!: true} = ip, queue, results)

  def run_component(component, ip, pipeline) do
    case component do
      %Clone{} = clone ->
        [ip, cloned_ip] = clone.__struct__.sync_process(ip, component)
        [first | _] = clone.to
        {sync_path, true} = path(pipeline, first.pid)
        cloned_ip = %{cloned_ip | sync_path: sync_path}
        [ip, cloned_ip]

      %Switch{} = switch ->
        case component.__struct__.sync_process(ip, component) do
          partition when is_atom(partition) ->
            [first | _] = Map.fetch!(component.branches, partition)
            {sync_path, true} = path(pipeline, first.pid)
            %{ip | sync_path: sync_path}

          error_ip ->
            error_ip
        end

      %Goto{} = goto ->
        case goto.__struct__.sync_process(ip, component) do
          {false, ip} ->
            ip

          {true, ip} ->
            goto_point = find_goto_point(pipeline, goto.to)
            {sync_path, true} = path(pipeline, goto_point)
            %{ip | sync_path: sync_path}
        end

      %DeadEnd{} ->
        nil

      %Plug{} = component ->
        component.__struct__.sync_process(ip, component)

      %Unplug{} = component ->
        component.__struct__.sync_process(ip, component)

      %GotoPoint{} = component ->
        component.__struct__.sync_process(ip, component)

      %Stage{} = component ->
        component.__struct__.sync_process(ip, component)

      %Decomposer{} = component ->
        component.__struct__.sync_process(ip, component)

      %Recomposer{} = component ->
        component.__struct__.sync_process(ip, component)

      _other ->
        raise "TODO"
    end
  end

  def path(pipeline, pid) do
    pipeline
    |> Enum.reduce({[], false}, fn component, {ref_list, found} ->
      case component do
        %Switch{branches: branches} = switch ->
          if found or pid == switch.pid do
            {ref_list ++ [switch.pid], true}
          else
            {inner_path, found_inside} =
              Enum.reduce(branches, {[], false}, fn {key, inner_pipeline},
                                                    {ref_list, found_in_branch} ->
                case path(inner_pipeline, pid) do
                  {path, true} ->
                    {ref_list ++ path, true}

                  {[], false} ->
                    {ref_list, found_in_branch}
                end
              end)

            if found_inside do
              {ref_list ++ inner_path, true}
            else
              {ref_list, false}
            end
          end

        %Clone{to: to_components} = clone ->
          if found or pid == clone.pid do
            {ref_list ++ [clone.pid], true}
          else
            {inner_path, found_inside} = path(to_components, pid)

            if found_inside do
              {ref_list ++ inner_path, true}
            else
              {ref_list, false}
            end
          end

        component ->
          if found or pid == component.pid do
            {ref_list ++ [component.pid], true}
          else
            {ref_list, false}
          end
      end
    end)
  end

  def find_component(pipeline, ref) do
    # TODO implement more performant Pipeline.find_component(ref)
    Enum.find(Pipeline.stages_to_list(pipeline), &(&1.pid == ref))
  end

  def find_goto_point(pipeline, name) do
    case Enum.filter(
           Pipeline.stages_to_list(pipeline),
           &(&1.name == name and &1.__struct__ == GotoPoint)
         ) do
      [component] ->
        component.pid

      [_component | _other] = components ->
        raise "Goto component error: found #{Enum.count(components)} components with name #{name}"

      [] ->
        raise "Goto component error: no component with name #{name}"
    end
  end
end
