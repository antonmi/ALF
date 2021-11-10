defmodule ALF.Manager.StreamTo do
  defmacro __using__(_opts) do
    quote do
      alias ALF.IP

      defmodule ProcessingOptions do
        defstruct [
          chunk_every: 10,
          return_ips: false
        ]

        def new(map) when is_map(map) do
          %__MODULE__{
            chunk_every: Map.get(map, :chunk_every, %__MODULE__{}.chunk_every),
            return_ips: Map.get(map, :return_ips, %__MODULE__{}.return_ips),
          }
        end
      end

      def stream_to(stream, name, opts \\ %{}) when is_atom(name) do
        GenServer.call(name, {:process_stream, stream, ProcessingOptions.new(opts)})
      end

      def handle_call({:process_stream, stream, opts}, _from, %__MODULE__{} = state) do
        stream_ref = make_ref()
        registry = Map.put(state.registry, stream_ref, %{inputs: %{}, queue: :queue.new})
        state = %{state | registry: registry}

        stream =
          stream
          |> build_input_stream(stream_ref, opts, state.name)
          |> build_output_stream(stream_ref, opts, state.name)

        {:reply, stream, state}
      end

      defp build_input_stream(stream, stream_ref, opts, manager_name) do
        stream
        |> Stream.chunk_every(opts.chunk_every)
        |> Stream.each(
             fn data ->
               data = Enum.map(data, &({make_ref(), &1}))
               send_data(manager_name, data, stream_ref)
             end
           )
      end

      defp build_output_stream(input_stream, stream_ref, opts, manager_name) do
        Stream.resource(
          fn ->
            Task.async(fn -> Stream.run(input_stream) end)
          end,
          fn (task) ->
            case flush_queue(manager_name, stream_ref) do
              {:ok, ips} ->
                format_output(ips, task, opts.return_ips)
              :done ->
                if Process.alive?(task.pid) do
                  {[], task}
                else
                  {:halt, task}
                end
            end
          end,
          fn _ -> :ok end
        )
      end

      defp format_output(ips, task, return_ips) do
        if return_ips do
          {ips, task}
        else
          results = Enum.map(ips, &(&1.datum))
          {results, task}
        end
      end

      defp send_data(name, data, stream_ref) when is_atom(name) and is_list(data) do
        GenServer.call(name, {:put_data_to_registry, data, stream_ref})

        ips = Enum.map(
          data,
          fn {ref, datum} ->
            %IP{
              stream_ref: stream_ref,
              ref: ref,
              init_datum: datum,
              datum: datum,
              manager_name: name
            }
          end
        )

        pipeline = __state__(name).pipeline
        GenServer.cast(pipeline.producer.pid, ips)
      end

      def handle_call({:put_data_to_registry, data, stream_ref}, _from, state) do
        stream_registry = state.registry[stream_ref]
        # TODO find better solution
        if Enum.count(stream_registry[:inputs]) + Enum.count(data) > 10_000 do
          Process.sleep(1)
        end

        inputs = Enum.reduce(
          data,
          stream_registry[:inputs],
          fn ({ref, datum}, inputs) ->
            Map.put(inputs, ref, datum)
          end
        )

        stream_reg = Map.put(stream_registry, :inputs, inputs)
        new_registry = Map.put(state.registry, stream_ref, stream_reg)

        {:reply, data, %{state | registry: new_registry}}
      end

      def result_ready(name, ip) when is_atom(name),
          do: GenServer.cast(name, {:result_ready, ip})

      def handle_cast({:result_ready, ip}, state) do
        stream_ref = ip.stream_ref
        stream_registry = state.registry[stream_ref]
        queue = :queue.in(ip, stream_registry[:queue])
        inputs = Map.delete(stream_registry[:inputs], ip.ref)

        new_registry = Map.put(
          state.registry,
          stream_ref,
          %{queue: queue, inputs: inputs}
        )
        {:noreply, %{state | registry: new_registry}}
      end

      defp flush_queue(name, stream_ref),
           do: GenServer.call(name, {:flush_queue, stream_ref})

      def handle_call({:flush_queue, stream_ref}, _from, state) do
        queue = state.registry[stream_ref][:queue]
        inputs = state.registry[stream_ref][:inputs]
        data = case :queue.to_list(queue) do
          [] ->
            if Enum.empty?(inputs) do
              :done
            else
              {:ok, []}
            end
          data when is_list(data) ->
            {:ok, data}
        end

        new_registry = Map.put(
          state.registry,
          stream_ref,
          %{queue: :queue.new, inputs: inputs}
        )
        {:reply, data, %{state | registry: new_registry}}
      end
    end
  end
end
