defmodule ALF.Manager.Components do
  @moduledoc """
    Utils function for working with pipeline components
  """

  alias ALF.Components.Stage

  def find_existing_workers(components, stage_set_ref) do
    Enum.filter(components, fn
      %Stage{} = stage ->
        stage.stage_set_ref == stage_set_ref

      _other ->
        false
    end)
  end

  def refresh_components_after_adding(
        components,
        new_stage,
        stages_to_subscribe_pids,
        subscribe_to_pids
      ) do
    components
    |> Enum.reduce([], fn component, acc ->
      cond do
        Enum.member?(subscribe_to_pids, component.pid) ->
          component = component.__struct__.__state__(component.pid)
          [component | acc]

        Enum.member?(stages_to_subscribe_pids, component.pid) ->
          GenStage.sync_subscribe(component.pid,
            to: new_stage.pid,
            max_demand: 1,
            cancel: :transient
          )

          component =
            Stage.add_subscribe_to(
              component,
              {new_stage.pid, max_demand: 1, cancel: :transient}
            )

          [component | acc]

        component.type == :stage and component.stage_set_ref == new_stage.stage_set_ref ->
          component = Stage.inc_count(component)

          if component.number == new_stage.number - 1 do
            [new_stage | [component | acc]]
          else
            [component | acc]
          end

        true ->
          [component | acc]
      end
    end)
    |> Enum.reduce([], fn component, acc ->
      if component.pid == new_stage.pid do
        [Stage.__state__(component.pid) | acc]
      else
        [component | acc]
      end
    end)
    |> Enum.reverse()
  end

  def refresh_components_before_removing(components, stage_to_delete) do
    components
    |> Enum.reduce([], fn component, acc ->
      cond do
        subscription =
            Enum.find(component.subscribers, fn {pid, _ref} -> pid == stage_to_delete.pid end) ->
          component = Stage.remove_subscriber(component, subscription)
          [component | acc]

        component.pid == stage_to_delete.pid ->
          acc

        component.type == :stage && component.stage_set_ref == stage_to_delete.stage_set_ref ->
          component = Stage.dec_count(component)
          [component | acc]

        subscribed_to =
            Enum.find(component.subscribed_to, fn {pid, _ref} ->
              pid == stage_to_delete.pid
            end) ->
          :ok = GenStage.cancel(subscribed_to, :shutdown)

          subscribe_to =
            Enum.find(component.subscribe_to, fn {pid, _opts} ->
              pid == stage_to_delete.pid
            end)

          component = Stage.remove_subscribe_to(component, subscribe_to)
          component = component.__struct__.__state__(component.pid)
          [component | acc]

        true ->
          [component | acc]
      end
    end)
    |> Enum.reverse()
  end
end
