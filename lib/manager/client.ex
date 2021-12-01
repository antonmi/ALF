defmodule ALF.Manager.Client do
  use GenServer

  alias ALF.Manager

  defstruct pipeline: nil, pid: nil, queue: :queue.new(), stream: nil

  def start(pipeline) do
    unless Process.whereis(pipeline) do
      raise "The #{pipeline} pipeline is not started, " <>
              "use ALF.Manager.start(#{pipeline}) before starting a client"
    end

    state = %__MODULE__{pipeline: pipeline}
    GenServer.start_link(__MODULE__, state)
  end

  def stop(pid) when is_pid(pid) do
    GenServer.call(pid, :stop)
  end

  def start_link(state) do
    GenServer.start_link(__MODULE__, state)
  end

  def init(%__MODULE__{} = state) do
    {:ok, %{state | pid: self()}, {:continue, :init_stream}}
  end

  def handle_continue(:init_stream, %__MODULE__{} = state) do
    {:noreply, init_stream(state)}
  end

  def put_to_queue(pid, event) do
    GenServer.cast(pid, {:put_to_queue, event})
  end

  def call(pid, event) do
    GenServer.cast(pid, {:put_to_queue, {event, self()}})

    receive do
      {:result, event} ->
        event
    end
  end

  def handle_cast({:put_to_queue, {event, pid}}, state) do
    queue = :queue.in({pid, event}, state.queue)
    {:noreply, %{state | queue: queue}}
  end

  def handle_call(:events, _from, state) do
    {:reply, :queue.to_list(state.queue), %{state | queue: :queue.new()}}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, state, state}
  end

  defp init_stream(state) do
    stream =
      Stream.resource(
        fn -> state.pid end,
        fn client_pid ->
          Process.sleep(10)
          try do
            events = GenServer.call(client_pid, :events)
            {events, client_pid}
          catch
            :exit, {_reason, _details} ->
              {:halt, client_pid}
          end
        end,
        fn _client_pid -> :ok end
      )
      |> Manager.steam_with_ids_to(state.pipeline, %{chunk_every: 1})
      |> Stream.each(fn {pid, event} ->
        send(pid, {:result, event})
      end)

    Task.async(fn -> Stream.run(stream) end)
    %{state | stream: stream}
  end
end
