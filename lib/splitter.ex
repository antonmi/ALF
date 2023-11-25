defmodule ALF.Splitter do
  use GenServer

  defstruct [:pid, :stream, :partitions, :running, :chunk_every]

  @chunk_every 1000

  def new(stream, partitions, opts \\ [])
      when is_function(stream) and is_list(partitions) and is_list(opts) do
    state = %__MODULE__{
      stream: stream,
      running: false,
      partitions: Enum.reduce(partitions, %{}, &Map.put(&2, &1, [])),
      chunk_every: Keyword.get(opts, :chunk_every, @chunk_every)
    }

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  def init(%__MODULE__{} = splitter) do
    {:ok, %{splitter | pid: self()}}
  end

  def stream(%__MODULE__{partitions: partitions} = splitter) do
    partitions
    |> Map.keys()
    |> Enum.map(fn partition ->
      Stream.resource(
        fn -> GenServer.call(splitter.pid, :run_stream) end,
        fn splitter ->
          case GenServer.call(splitter.pid, {:get_data, partition}) do
            {:ok, data} ->
              {data, splitter}

            {:error, :done} ->
              {:halt, splitter}
          end
        end,
        fn splitter -> splitter end
      )
    end)
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  defp async_run_stream(stream, chunk_every, pid) do
    Task.async(fn ->
      stream
      |> Stream.chunk_every(chunk_every)
      |> Stream.each(fn chunk ->
        data_size = GenServer.call(pid, {:new_data, chunk})
        maybe_wait(data_size, chunk_every)
      end)
      |> Stream.run()

      GenServer.call(pid, :done)
    end)
  end

  defp maybe_wait(data_length, chunk_every) do
    if data_length > 10 * chunk_every do
      div = div(data_length, 10 * chunk_every)
      to_sleep = trunc(:math.pow(2, div))
      Process.sleep(to_sleep)
    end
  end

  def handle_call({:new_data, data}, _from, %__MODULE__{} = splitter) do
    new_partitions =
      Enum.reduce(splitter.partitions, %{}, fn {fun, prev_data}, acc ->
        case Enum.split_with(data, fun) do
          {[], _} ->
            Map.put(acc, fun, prev_data)

          {data, _} ->
            new_data = prev_data ++ data
            Map.put(acc, fun, new_data)
        end
      end)

    data_size =
      Enum.reduce(new_partitions, 0, fn {_key, data}, acc -> acc + length(data) end)

    {:reply, data_size, %{splitter | partitions: new_partitions}}
  end

  def handle_call(:run_stream, _from, %__MODULE__{} = splitter) do
    if splitter.running do
      {:reply, splitter, splitter}
    else
      async_run_stream(splitter.stream, splitter.chunk_every, splitter.pid)
      splitter = %{splitter | running: true}
      {:reply, splitter, splitter}
    end
  end

  def handle_call(:done, _from, %__MODULE__{} = splitter) do
    {:reply, :ok, %{splitter | running: false}}
  end

  def handle_call(
        {:get_data, partition},
        _from,
        %__MODULE__{partitions: partitions, running: running} = splitter
      ) do
    data = Map.get(partitions, partition)

    if length(data) == 0 && !running do
      {:reply, {:error, :done}, splitter}
    else
      {:reply, {:ok, data}, %{splitter | partitions: Map.put(partitions, partition, [])}}
    end
  end

  def handle_call(:__state__, _from, splitter), do: {:reply, splitter, splitter}

  def handle_info({_task_ref, :ok}, mixer) do
    # do nothing for now
    {:noreply, mixer}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, mixer) do
    # do nothing for now
    {:noreply, mixer}
  end
end
