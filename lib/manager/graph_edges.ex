defmodule ALF.Manager.GraphEdges do
  defmacro __using__(_opts) do
    quote do
      alias ALF.Components.{Stage, Goto}

      def graph_edges(name) when is_atom(name), do: GenServer.call(name, :graph_edges)

      def handle_call(:graph_edges, _from, state) do
        pid_to_name =
          state.components
          |> Enum.reduce(%{}, fn stage, acc ->
            name =
              case stage do
                %Stage{name: name, number: number} ->
                  "#{stage.pipe_module}-#{stage.name}_#{stage.number}"

                stage ->
                  "#{stage.pipe_module}-#{stage.name}"
              end

            Map.put(acc, stage.pid, "#{String.replace(name, ~r/^Elixir\./, "")}")
          end)

        edges =
          state.components
          |> Enum.flat_map(fn stage ->
            target = Map.get(pid_to_name, stage.pid)

            Enum.map(stage.subscribe_to, fn {pid, _} ->
              {Map.get(pid_to_name, pid), target}
            end)
          end)

        goto_edges =
          state.components
          |> Enum.filter(&(&1.__struct__ == Goto))
          |> Enum.reduce([], fn goto, acc ->
            target = Map.get(pid_to_name, goto.to_pid)
            acc ++ [{Map.get(pid_to_name, goto.pid), target}]
          end)

        {:reply, edges ++ goto_edges, state}
      end
    end
  end
end
