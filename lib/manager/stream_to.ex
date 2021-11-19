defmodule ALF.Manager.StreamTo do
  defmacro __using__(_opts) do
    quote do
      alias ALF.{IP, ErrorIP, Manager.StreamRegistry}

      defmodule ProcessingOptions do
        defstruct chunk_every: 10,
                  return_ips: false

        def new(map) when is_map(map) do
          struct(__MODULE__, map)
        end
      end

      @spec stream_to(Enumerable.t(), atom(), map() | keyword()) :: Enumerable.t()
      def stream_to(stream, name, opts \\ %{}) when is_atom(name) do
        GenServer.call(name, {:stream_to, stream, ProcessingOptions.new(opts)})
      end

      def add_to_registry(name, ips, stream_ref) do
        GenServer.call(name, {:add_to_registry, ips, stream_ref})
      end

      def remove_from_registry(name, ips, stream_ref) do
        GenServer.call(name, {:remove_from_registry, ips, stream_ref})
      end

      def handle_call({:stream_to, stream, opts}, _from, %__MODULE__{} = state) do
        stream_ref = make_ref()
        registry = Map.put(state.registry, stream_ref, %StreamRegistry{inputs: %{}, queue: :queue.new()})
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
        |> Stream.each(fn data ->
          send_data(manager_name, data, stream_ref)
        end)
      end

      defp build_output_stream(input_stream, stream_ref, opts, manager_name) do
        Stream.resource(
          fn ->
            Task.async(fn -> Stream.run(input_stream) end)
          end,
          fn task ->
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

      defp format_output([%IP{} | _] = ips, task, true), do: {ips, task}

      defp format_output([%IP{} | _] = ips, task, false) do
        {Enum.map(ips, & &1.datum), task}
      end

      defp format_output([%ErrorIP{} | _] = ips, task, _return_ips), do: {ips, task}
      defp format_output([], task, return_ips), do: {[], task}

      defp send_data(name, data, stream_ref) when is_atom(name) and is_list(data) do
        ips = build_ips(data, stream_ref, name)
        add_to_registry(name, ips, stream_ref)
        pipeline = __state__(name).pipeline
        GenServer.cast(pipeline.producer.pid, ips)
      catch
        :exit, {reason, details} ->
          {:exit, {reason, details}}
      end

      defp resend_packets(%__MODULE__{} = state) do
        new_registry =
          state.registry
          |> Enum.each(fn {stream_ref, %StreamRegistry{inputs: inputs, queue: queue}} ->
            ips = build_ips(inputs, stream_ref, state.name)
            GenServer.cast(state.pipeline.producer.pid, ips)
          end)

        state
      end

      def build_ips(data, stream_ref, name) do
        Enum.map(
          data,
          fn datum ->
            {reference, datum} = case datum do
              {ref, dat} when is_reference(ref) ->
                {ref, dat}
              dat ->
                {make_ref(), dat}
            end

            %IP{
              stream_ref: stream_ref,
              ref: reference,
              init_datum: datum,
              datum: datum,
              manager_name: name
            }
          end
        )
      end

      def handle_call({:add_to_registry, ips, stream_ref}, _from, state) do
        stream_registry = state.registry[stream_ref]
        # TODO find better solution
        if Enum.count(stream_registry.inputs) + Enum.count(ips) > 1_000 do
          Process.sleep(1)
        end

        inputs =
          Enum.reduce(
            ips,
            stream_registry.inputs,
            fn ip, inputs ->
              Map.put(inputs, ip.ref, ip.datum)
            end
          )

        stream_reg = %{stream_registry | inputs: inputs}
        new_registry = Map.put(state.registry, stream_ref, stream_reg)

        {:reply, ips, %{state | registry: new_registry}}
      end

      def handle_call({:remove_from_registry, ips, stream_ref}, _from, state) do
        stream_registry = state.registry[stream_ref]

        inputs =
          Enum.reduce(
            ips,
            stream_registry.inputs,
            fn ip, inputs ->
              Map.delete(inputs, ip.ref)
            end
          )

        stream_reg = %{stream_registry | inputs: inputs}
        new_registry = Map.put(state.registry, stream_ref, stream_reg)

        {:reply, ips, %{state | registry: new_registry}}
      end

      def result_ready(name, ip) when is_atom(name),
        do: GenServer.call(name, {:result_ready, ip})

      def handle_call({:result_ready, ip}, _from, state) do
        stream_ref = ip.stream_ref
        stream_registry = state.registry[stream_ref]
        queue = :queue.in(ip, stream_registry.queue)
        inputs = Map.delete(stream_registry.inputs, ip.ref)

        new_registry =
          Map.put(
            state.registry,
            stream_ref,
            %StreamRegistry{queue: queue, inputs: inputs}
          )

        {:reply, :ok, %{state | registry: new_registry}}
      end

      defp flush_queue(name, stream_ref) do
        GenServer.call(name, {:flush_queue, stream_ref})
      catch
        :exit, {:normal, _details} ->
          :done
      end

      def handle_call({:flush_queue, stream_ref}, _from, state) do
        queue = state.registry[stream_ref].queue
        inputs = state.registry[stream_ref].inputs

        data =
          case :queue.to_list(queue) do
            [] ->
              if Enum.empty?(inputs) do
                :done
              else
                {:ok, []}
              end

            data when is_list(data) ->
              {:ok, data}
          end

        new_registry =
          Map.put(
            state.registry,
            stream_ref,
            %StreamRegistry{queue: :queue.new(), inputs: inputs}
          )

        {:reply, data, %{state | registry: new_registry}}
      end
    end
  end
end
