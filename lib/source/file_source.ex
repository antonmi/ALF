defmodule ALF.Source.FileSource do
  @behaviour ALF.Source

  use GenServer

  defstruct [:path, :pid, :file, :rest, :chunk_size, :line_sep, :wait]

  @chunk_size 10_000
  @line_sep "\n"

  @impl true
  def open(path, opts \\ []) do
    state = %__MODULE__{
      path: path,
      chunk_size: Keyword.get(opts, :chunk_size, @chunk_size),
      line_sep: Keyword.get(opts, :line_sep, @line_sep),
      wait: Keyword.get(opts, :wait, false)
    }

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{path: path} = state) do
    state = %{state | file: File.open!(path), rest: "", pid: self()}
    {:ok, state}
  end

  @impl true
  def stream(%__MODULE__{} = source) do
    Stream.resource(
      fn -> source end,
      fn source ->
        case call(source) do
          {:ok, lines} ->
            {lines, source}

          {:error, :eof} ->
            case source.wait do
              true -> {[], source}
              false -> {:halt, source}
            end
        end
      end,
      fn source -> File.close(source.path) end
    )
  end

  @impl true
  def close(%__MODULE__{path: path, pid: pid}) do
    File.close(path)
    GenServer.call(pid, :stop)
  end

  def call(%__MODULE__{pid: pid}), do: GenServer.call(pid, :call)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:call, _from, %__MODULE__{} = state) do
    case read_lines(state.file, state.chunk_size, state.line_sep) do
      {:ok, {[head | tail], last}} ->
        {:reply, {:ok, [state.rest <> head | tail]}, %{state | rest: last}}

      {:ok, {[], last}} ->
        {:reply, {:ok, []}, %{state | rest: state.rest <> last}}

      {:error, :eof} ->
        if String.length(state.rest) > 0 do
          {:reply, {:ok, [state.rest]}, %{state | rest: ""}}
        else
          {:reply, {:error, :eof}, state}
        end
    end
  end

  def handle_call(:stop, _from, state), do: {:stop, :normal, state}

  def handle_call(:__state__, _from, state), do: {:reply, state, state}

  defp read_lines(file, chunk_size, line_sep) do
    case IO.read(file, chunk_size) do
      data when is_binary(data) ->
        [last | lines] = Enum.reverse(String.split(data, line_sep))
        {:ok, {Enum.reverse(lines), last}}

      _ ->
        {:error, :eof}
    end
  end
end
