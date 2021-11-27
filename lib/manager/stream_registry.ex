defmodule ALF.Manager.StreamRegistry do
  defstruct ref: nil,
            queue: :queue.new(),
            inputs: %{},
            in_progress: %{},
            decomposed: %{},
            recomposed: %{}

  def empty?(%__MODULE__{} = registry) do
    Enum.empty?(registry.inputs) and
      Enum.empty?(registry.in_progress) and
      Enum.empty?(registry.decomposed) and
      Enum.empty?(registry.recomposed)
  end

  def add_to_registry(%__MODULE__{} = registry, ips) do
    {inputs, in_progress, decomposed, recomposed} =
      Enum.reduce(
        ips,
        {registry.inputs, registry.in_progress, registry.decomposed, registry.recomposed},
        fn ip, {inputs, in_progress, decomposed, recomposed} ->
          cond do
            ip.in_progress ->
              {inputs, Map.put(in_progress, ip.ref, ip.event), decomposed, recomposed}

            ip.decomposed ->
              {inputs, in_progress, Map.put(decomposed, ip.ref, ip.event), recomposed}

            ip.recomposed ->
              {inputs, in_progress, decomposed, Map.put(recomposed, ip.ref, ip.event)}

            true ->
              {Map.put(inputs, ip.ref, ip.event), in_progress, decomposed, recomposed}
          end
        end
      )

    %{
      registry
      | inputs: inputs,
        in_progress: in_progress,
        decomposed: decomposed,
        recomposed: recomposed
    }
  end

  def remove_from_registry(%__MODULE__{} = registry, ips) do
    {inputs, in_progress, decomposed, recomposed} =
      Enum.reduce(
        ips,
        {registry.inputs, registry.in_progress, registry.decomposed, registry.recomposed},
        fn ip, {inputs, in_progress, decomposed, recomposed} ->
          cond do
            ip.in_progress ->
              {inputs, Map.delete(in_progress, ip.ref), decomposed, recomposed}

            ip.decomposed ->
              {inputs, in_progress, Map.delete(decomposed, ip.ref), recomposed}

            ip.recomposed ->
              {inputs, in_progress, decomposed, Map.delete(recomposed, ip.ref)}

            true ->
              {Map.delete(inputs, ip.ref), in_progress, decomposed, recomposed}
          end
        end
      )

    %{
      registry
      | inputs: inputs,
        in_progress: in_progress,
        decomposed: decomposed,
        recomposed: recomposed
    }
  end
end
