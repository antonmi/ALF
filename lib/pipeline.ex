defmodule ALF.Pipeline do
  defstruct module: nil,
            components: [],
            producer: nil,
            consumer: nil

  alias ALF.Components.{Switch, Clone}

  def stages_to_list(components) do
    do_stages_to_list(components, [])
  end

  def find_component_by_pid(components, pid) do
    try do
      :ok = do_find_component_by_pid(components, pid)
      nil
    catch
      component ->
        component
    end
  end
  
  def do_find_component_by_pid(components, pid) do
    Enum.each(components, fn component ->
      if component.pid == pid do
        throw(component)
      else
        case component do
          %Switch{branches: branches} = switch ->
            Enum.each(branches, fn {_key, partition_comps} ->
              do_find_component_by_pid(partition_comps, pid)
            end)

          %Clone{to: to_components} = stage ->
            do_find_component_by_pid(to_components, pid)

          component ->
            nil
        end
      end
    end)
  end

  defp do_stages_to_list(components, list) do
    Enum.reduce(components, list, fn stage, found ->
      found ++
        case stage do
          %Switch{branches: branches} = stage ->
            [stage] ++
              Enum.reduce(branches, [], fn {_key, partition_stages}, inner_found ->
                inner_found ++ do_stages_to_list(partition_stages, [])
              end)

          %Clone{to: pipe_stages} = stage ->
            [stage] ++ do_stages_to_list(pipe_stages, [])

          stage ->
            [stage]
        end
    end)
  end
end
