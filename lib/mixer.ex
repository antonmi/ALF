defmodule ALF.Mixer do
  use GenServer

  defstruct [:streams, :pid, :running, :data, :chunk_every]

  @chunk_every 1000

  def new(streams, opts \\ [])

  def new(streams, opts) when is_list(streams) do
    state = %__MODULE__{
      streams: streams,
      running: false,
      chunk_every: Keyword.get(opts, :chunk_every, @chunk_every)
    }

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  def new(stream, opts) when is_function(stream) do
    new([stream], opts)
  end

  def init(%__MODULE__{streams: streams} = mixer) do
    {:ok, %{mixer | pid: self(), data: [], streams: MapSet.new(streams)}}
  end

  def add(%__MODULE__{pid: pid}, stream), do: GenServer.call(pid, {:add, stream})

  def stream(%__MODULE__{} = mixer) do
    :ok = GenServer.call(mixer.pid, :running)
    :ok = GenServer.call(mixer.pid, :run_streams)

    Stream.resource(
      fn -> mixer end,
      fn mixer ->
        case GenServer.call(mixer.pid, :get_data) do
          {:ok, data} ->
            {data, mixer}

          {:error, :done} ->
            {:halt, mixer}
        end
      end,
      fn mixer -> mixer end
    )
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  defp run_streams(streams, pid, chunk_every) do
    Enum.map(streams, fn stream ->
      async_run_stream(stream, chunk_every, pid)
    end)
  end

  defp async_run_stream(stream, chunk_every, pid) do
    Task.async(fn ->
      stream
      |> Stream.chunk_every(chunk_every)
      |> Stream.each(fn chunk ->
        data_length = GenServer.call(pid, {:new_data, chunk})
        maybe_wait(data_length, chunk_every)
      end)
      |> Stream.run()

      GenServer.call(pid, {:done, stream})
    end)
  end

  defp maybe_wait(data_length, chunk_every) do
    if data_length > 10 * chunk_every do
      div = div(data_length, 10 * chunk_every)
      to_sleep = trunc(:math.pow(2, div))
      Process.sleep(to_sleep)
    end
  end

  def handle_call({:new_data, data}, _from, %__MODULE__{data: prev_data} = mixer) do
    data = data ++ prev_data

    {:reply, length(data), %{mixer | data: data}}
  end

  def handle_call(:run_streams, _from, %__MODULE__{} = mixer) do
    run_streams(mixer.streams, mixer.pid, mixer.chunk_every)

    {:reply, :ok, %{mixer | running: true}}
  end

  def handle_call(:running, _from, %__MODULE__{} = mixer) do
    {:reply, :ok, %{mixer | running: true}}
  end

  def handle_call({:done, stream}, _from, %__MODULE__{streams: streams} = mixer) do
    streams = MapSet.delete(streams, stream)
    {:reply, :ok, %{mixer | streams: streams}}
  end

  def handle_call(:get_data, _from, %__MODULE__{data: data, streams: streams} = mixer) do
    if length(data) == 0 && MapSet.size(streams) == 0 do
      {:reply, {:error, :done}, mixer}
    else
      {:reply, {:ok, data}, %{mixer | data: []}}
    end
  end

  def handle_call({:add, stream}, _from, %__MODULE__{streams: streams} = mixer) do
    mixer = %{mixer | streams: MapSet.put(streams, stream)}

    if mixer.running do
      async_run_stream(stream, mixer.chunk_every, mixer.pid)
    end

    {:reply, mixer, mixer}
  end

  def handle_call(:__state__, _from, mixer), do: {:reply, mixer, mixer}

  def handle_info({_task_ref, :ok}, mixer) do
    # do nothing for now
    {:noreply, mixer}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, mixer) do
    # do nothing for now
    {:noreply, mixer}
  end
end
