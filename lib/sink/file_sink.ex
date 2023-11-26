defmodule ALF.Sink.FileSink do
  use GenServer

  @behaviour ALF.Sink

  defstruct [:path, :pid, :file, :line_sep]

  @line_sep "\n"

  @impl true
  def open(path, opts \\ []) do
    state = %__MODULE__{
      path: path,
      line_sep: Keyword.get(opts, :line_sep, @line_sep)
    }

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{path: path} = state) do
    state = %{state | file: File.open!(path, [:write]), pid: self()}
    {:ok, state}
  end

  @impl true
  def stream(stream, %__MODULE__{} = source) do
    stream
    |> Stream.transform(
      fn -> source end,
      fn el, source ->
        call(source, el)
        {[el], source}
      end,
      fn source -> File.close(source.path) end
    )
  end

  @impl true
  def close(%__MODULE__{path: path, pid: pid}) do
    File.close(path)
    GenServer.call(pid, :stop)
  end

  def call(%__MODULE__{pid: pid}, data), do: GenServer.call(pid, {:call, data})

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call({:call, data}, _from, %__MODULE__{file: file} = state) do
    IO.write(file, data <> state.line_sep)

    {:reply, :ok, state}
  end

  def handle_call(:stop, _from, state), do: {:stop, :normal, state}

  def handle_call(:__state__, _from, state), do: {:reply, state, state}
end
