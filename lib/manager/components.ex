defmodule ALF.Manager.Components do
  @moduledoc """
    Utils function for working with pipeline components
  """

  alias ALF.Components.Stage
  alias ALF.Builder

  def add_component(components, stage_set_ref, pipeline_sup_pid) do
    existing_workers = find_and_reload_existing_workers(components, stage_set_ref)

    new_stage = Builder.add_stage_worker(pipeline_sup_pid, existing_workers)

    stages_to_subscribe_pids = Enum.map(new_stage.subscribers, fn {pid, _opts} -> pid end)

    new_components =
      components
      |> Enum.reduce([], fn component, acc ->
        cond do
          Enum.member?(stages_to_subscribe_pids, component.pid) ->
            GenStage.sync_subscribe(component.pid,
              to: new_stage.pid,
              max_demand: 1,
              cancel: :transient
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
      |> Enum.reverse()

    {new_stage, new_components}
  end

  def remove_component(components, stage_set_ref) do
    existing_workers = find_and_reload_existing_workers(components, stage_set_ref)

    if length(existing_workers) > 1 do
      stage_to_delete = Enum.max_by(existing_workers, & &1.number)

      Enum.map(stage_to_delete.subscribed_to, fn subscription ->
        :ok = GenStage.cancel(subscription, :shutdown)
      end)

      new_components =
        components
        |> Enum.reduce([], fn component, acc ->
          cond do
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
              [component | acc]

            true ->
              [component | acc]
          end
        end)
        |> Enum.reverse()

      {:ok, {stage_to_delete, new_components}}
    else
      {:error, :only_one_left}
    end
  end

  defp find_and_reload_existing_workers(components, stage_set_ref) do
    Enum.reduce(components, [], fn
      %Stage{stage_set_ref: ^stage_set_ref, pid: pid}, acc ->
        [Stage.__state__(pid) | acc]

      _other, acc ->
        acc
    end)
  end
end
