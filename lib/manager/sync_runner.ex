defmodule ALF.Manager.SyncRunner do
  alias ALF.Components.{
    Goto,
    GotoPoint,
    Switch,
    Clone
  }

  alias ALF.Pipeline
  alias ALF.{ErrorIP, IP}

  def run([first | _] = pipeline, %IP{sync_path: nil} = ip) do
    {path, true} = path(pipeline, first.pid)
    ip = %{ip | sync_path: path}
    run(pipeline, ip)
  end

  def run(pipeline, ip) do
    do_run(pipeline, ip, [], [])
  end

  defp do_run(pipeline, %IP{sync_path: sync_path, done!: false} = ip, queue, results) do
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

  defp do_run(pipeline, %IP{done!: true} = ip, queue, results) do
    do_run(pipeline, nil, queue, [ip | results])
  end

  defp do_run(_pipeline, nil, [], results), do: Enum.reverse(results)

  defp do_run(pipeline, nil, [ip | ips], results) do
    do_run(pipeline, ip, ips, results)
  end

  defp do_run(pipeline, %ErrorIP{} = error_ip, queue, results) do
    do_run(pipeline, nil, queue, [error_ip | results])
  end

  def run_component(component, ip, pipeline) do
    case component do
      %Clone{} = clone ->
        [ip, cloned_ip] = clone.__struct__.sync_process(ip, component)
        [first | _] = clone.to
        {sync_path, true} = path(pipeline, first.pid)
        cloned_ip = %{cloned_ip | sync_path: sync_path}
        [ip, cloned_ip]

      %Switch{} = switch ->
        case switch.__struct__.sync_process(ip, switch) do
          partition when is_atom(partition) ->
            [first | _] = Map.fetch!(switch.branches, partition)
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

      component ->
        component.__struct__.sync_process(ip, component)
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
              Enum.reduce(branches, {[], false}, fn {_key, inner_pipeline},
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
    Pipeline.find_component_by_pid(pipeline, ref)
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
