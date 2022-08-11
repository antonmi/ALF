defmodule ALF.Manager.Streamer do
  alias ALF.{
    IP,
    ErrorIP,
    Manager,
    Manager.StreamRegistry,
    Components.Producer
  }

  def call_add_to_registry(name, ips, stream_ref) when is_list(ips) do
    GenServer.call(name, {:add_to_registry, ips, stream_ref})
  end

  def call_remove_from_registry(name, ips, stream_ref) do
    GenServer.call(name, {:remove_from_registry, ips, stream_ref})
  end

  def cast_result_ready(name, ip) when is_atom(name) do
    GenServer.cast(name, {:result_ready, ip})
  end

  def add_to_registry(state_registry, stream_ref, ips) do
    stream_registry = state_registry[stream_ref]
    stream_reg = StreamRegistry.add_to_registry(stream_registry, ips)
    Map.put(state_registry, stream_ref, stream_reg)
  end

  def remove_from_registry(state_registry, stream_ref, ips) do
    stream_registry = state_registry[stream_ref]

    stream_reg = StreamRegistry.remove_from_registry(stream_registry, ips)
    Map.put(state_registry, stream_ref, stream_reg)
  end

  def prepare_streams(state, stream, opts, custom_ids?) do
    stream_ref = make_ref()

    registry =
      Map.put(state.registry, stream_ref, %StreamRegistry{
        queue: :queue.new(),
        ref: stream_ref
      })

    state = %{state | registry: registry}

    stream =
      transform_stream(stream, stream_ref, opts, state.name, state.producer_pid, custom_ids?)

    {stream, state}
  end

  def transform_stream(stream, stream_ref, opts, manager_name, producer_pid, custom_ids?) do
    stream
    |> Stream.chunk_every(opts.chunk_every)
    |> Stream.chunk_while(
      [],
      fn events, acc ->
        send_events(manager_name, events, stream_ref, producer_pid)
        Process.sleep(10)
        {_any_status, ips} = call_flush_queue(manager_name, stream_ref)
        acc = acc ++ format_output(ips, opts.return_ips, custom_ids?)
        {:cont, acc, []}
      end,
      fn [] ->
        ips = wait_for_done(manager_name, stream_ref, [])
        {:cont, format_output(ips, opts.return_ips, custom_ids?), []}
      end
    )
    |> Stream.flat_map(& &1)
  end

  def wait_for_done(manager_name, stream_ref, acc) do
    case call_flush_queue(manager_name, stream_ref) do
      {:not_done_yet, ips} ->
        Process.sleep(10)
        wait_for_done(manager_name, stream_ref, acc ++ ips)

      {:done, ips} ->
        acc ++ ips
    end
  end

  def rebuild_registry_on_result_ready(state_registry, ip) do
    stream_ref = ip.stream_ref
    stream_registry = state_registry[stream_ref]
    new_stream_registry = StreamRegistry.remove_from_registry(stream_registry, [ip])
    queue = :queue.in(ip, new_stream_registry.queue)

    new_stream_registry = %{new_stream_registry | queue: queue}

    Map.put(state_registry, stream_ref, new_stream_registry)
  end

  def format_output(ips, return_ips?, custom_ids?) do
    ips = if custom_ids?, do: Enum.map(ips, &{&1.ref, &1}), else: ips

    Enum.map(ips, fn ip ->
      format_ip(ip, return_ips?)
    end)
  end

  defp format_ip(%IP{} = ip, true), do: ip
  defp format_ip({id, %IP{} = ip}, true), do: {id, ip}
  defp format_ip(%IP{} = ip, false), do: ip.event
  defp format_ip({id, %IP{} = ip}, false), do: {id, ip.event}

  defp format_ip(%ErrorIP{} = ip, _return_ips), do: ip
  defp format_ip({id, %ErrorIP{} = ip}, _return_ips), do: {id, ip}

  defp send_events(name, events, stream_ref, producer_pid)
       when is_atom(name) and is_list(events) do
    ips = build_ips(events, stream_ref, name)
    call_add_to_registry(name, ips, stream_ref)
    :ok = wait_until_producer_load_is_ok(producer_pid)
    Producer.load_ips(producer_pid, ips)
  catch
    :exit, {reason, details} ->
      {:exit, {reason, details}}
  end

  defp wait_until_producer_load_is_ok(producer_pid) do
    if Producer.ips_count(producer_pid) < Manager.max_producer_load() do
      :ok
    else
      Process.sleep(10)
      wait_until_producer_load_is_ok(producer_pid)
    end
  end

  def resend_packets(%Manager{} = state) do
    new_registry =
      state.registry
      |> Enum.reduce(%{}, fn {stream_ref, %StreamRegistry{} = registry}, acc ->
        ips = build_ips(registry.in_progress, stream_ref, state.name)
        Producer.load_ips(state.producer_pid, ips)
        # forget about composed and recomposed currently
        Map.put(acc, stream_ref, registry)
      end)

    %{state | registry: new_registry}
  end

  def build_ips(events, stream_ref, name) do
    Enum.map(
      events,
      fn event ->
        case event do
          {id, event} ->
            %IP{
              in_progress: true,
              stream_ref: stream_ref,
              ref: id,
              init_datum: event,
              event: event,
              manager_name: name
            }

          event ->
            {reference, event} =
              case event do
                {ref, dat} when is_reference(ref) ->
                  {ref, dat}

                dat ->
                  {make_ref(), dat}
              end

            %IP{
              in_progress: true,
              stream_ref: stream_ref,
              ref: reference,
              init_datum: event,
              event: event,
              manager_name: name
            }
        end
      end
    )
  end

  defp call_flush_queue(name, stream_ref) do
    GenServer.call(name, {:flush_queue, stream_ref}, :infinity)
  catch
    :exit, {:normal, _details} ->
      {:done, []}

    :exit, {:noproc, _details} ->
      {:done, []}
  end
end
