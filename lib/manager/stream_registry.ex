defmodule ALF.Manager.StreamRegistry do
  defstruct ref: nil, inputs: %{}, queue: :queue.new(), decomposed: %{}, recomposed: %{}

  def empty?(%__MODULE__{} = registry) do
    Enum.empty?(registry.inputs) and
      Enum.empty?(registry.decomposed) and
      Enum.empty?(registry.recomposed)
  end

  def add_to_registry(%__MODULE__{} = registry, ips) do
    {inputs, decomposed, recomposed} =
      Enum.reduce(
        ips,
        {registry.inputs, registry.decomposed, registry.recomposed},
        fn ip, {inputs, decomposed, recomposed} ->
          cond do
            ip.decomposed ->
              {inputs, Map.put(decomposed, ip.ref, ip.datum), recomposed}

            ip.recomposed ->
              {inputs, decomposed, Map.put(recomposed, ip.ref, ip.datum)}

            true ->
              {Map.put(inputs, ip.ref, ip.datum), decomposed, recomposed}
          end
        end
      )

    %{registry | inputs: inputs, decomposed: decomposed, recomposed: recomposed}
  end

  def remove_from_registry(%__MODULE__{} = registry, ips) do
    {inputs, decomposed, recomposed} =
      Enum.reduce(
        ips,
        {registry.inputs, registry.decomposed, registry.recomposed},
        fn ip, {inputs, decomposed, recomposed} ->
          cond do
            ip.decomposed ->
              {inputs, Map.delete(decomposed, ip.ref), recomposed}

            ip.recomposed ->
              {inputs, decomposed, Map.delete(recomposed, ip.ref)}

            true ->
              {Map.delete(inputs, ip.ref), decomposed, recomposed}
          end
        end
      )

    %{registry | inputs: inputs, decomposed: decomposed, recomposed: recomposed}
  end
end
