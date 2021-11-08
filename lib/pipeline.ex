defmodule ALF.Pipeline do
  defstruct [
    module: nil,
    stages: [],
    producer: nil,
    consumer: nil
  ]

  alias ALF.{Switch, Clone}

  def stages_to_list(stages) do
    do_stages_to_list(stages, [])
  end

  defp do_stages_to_list(stages, list) do
    Enum.reduce(stages, list, fn(stage, found) ->
      found ++ case stage do
        %Switch{partitions: partitions} = stage ->
          [stage] ++ Enum.reduce(partitions, [], fn({_key, partition_stages}, inner_found) ->
            inner_found ++ do_stages_to_list(partition_stages, [])
          end)
        %Clone{to: pipe_stages} = stage->
          [stage] ++ do_stages_to_list(pipe_stages, [])
        stage ->
          [stage]
      end
    end)
  end
end
