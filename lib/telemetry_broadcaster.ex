defmodule ALF.TelemetryBroadcaster do
  use GenServer

  defmodule Handler do
    def handle_event([:alf, :component, type], measurements, metadata, %{pid: pid}) do
      GenServer.cast(pid, {:handle_event, [:alf, :component, type], measurements, metadata})
    end
  end

  defstruct remote_function: nil,
            pid: nil,
            memo: %{}

  @allowed_opts [:interval]

  @spec start_link([]) :: GenServer.on_start()
  def start_link([]) do
    GenServer.start_link(__MODULE__, %__MODULE__{}, name: __MODULE__)
  end

  @impl true
  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    {:ok, state, {:continue, :attach_telemetry}}
  end

  @impl true
  def handle_continue(:attach_telemetry, %__MODULE__{} = state) do
    do_attach_telemetry(state.pid)
    {:noreply, state}
  end

  @spec register_remote_function(atom(), atom(), atom(), list()) :: {atom(), atom(), atom()}
  def register_remote_function(name, module, function, opts \\ [interval: nil])
      when is_atom(name) and is_atom(module) and is_atom(function) and is_list(opts) do
    wrong_options = Keyword.keys(opts) -- @allowed_opts

    if Enum.any?(wrong_options) do
      raise "Wrong options for TelemetryBroadcaster: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@allowed_opts)}"
    end

    GenServer.call(__MODULE__, {:register_remote_function, {name, module, function, opts}})
  end

  def remote_function(), do: GenServer.call(__MODULE__, :remote_function)

  @impl true
  def handle_call(:remote_function, _from, state), do: {:reply, state.remote_function, state}

  def handle_call({:register_remote_function, {name, module, function, opts}}, _from, state) do
    state = %{state | remote_function: {name, module, function, opts}}
    {:reply, {name, module, function, opts}, state}
  end

  @impl true
  def handle_cast({:handle_event, [:alf, :component, _], _, _}, %{remote_function: nil} = state) do
    {:noreply, state}
  end

  def handle_cast({:handle_event, [:alf, :component, type], measurements, metadata}, state) do
    {name, module, function, opts} = state.remote_function

    if opts[:interval] && opts[:interval] > 0 do
      do_handle_cast_with_interval(
        {type, measurements, metadata},
        {name, module, function},
        opts[:interval],
        state
      )
    else
      :rpc.call(name, module, function, [[:alf, :component, type], measurements, metadata])
      {:noreply, state}
    end
  end

  defp do_handle_cast_with_interval(
         {type, measurements, metadata},
         {name, module, function},
         interval,
         state
       ) do
    {component_name, pipeline, ip_ref} = fetch_data_from_meta(metadata)
    {timestamp, producer_ip_ref} = Map.get(state.memo, pipeline, {false, false})

    if component_name == :producer and type == :start do
      time_now = Time.utc_now()

      if timestamp do
        if Time.diff(time_now, timestamp, :millisecond) > interval do
          :rpc.call(name, module, function, [[:alf, :component, type], measurements, metadata])
          new_memo = Map.put(state.memo, pipeline, {time_now, ip_ref})
          {:noreply, %{state | memo: new_memo}}
        else
          {:noreply, state}
        end
      else
        :rpc.call(name, module, function, [[:alf, :component, type], measurements, metadata])
        new_memo = Map.put(state.memo, pipeline, {time_now, ip_ref})
        {:noreply, %{state | memo: new_memo}}
      end
    else
      if ip_ref == producer_ip_ref do
        :rpc.call(name, module, function, [[:alf, :component, type], measurements, metadata])
        {:noreply, state}
      else
        {:noreply, state}
      end
    end
  end

  defp fetch_data_from_meta(metadata) do
    {
      get_in(metadata, [:component, :name]),
      get_in(metadata, [:component, :pipeline_module]),
      get_in(metadata, [:ip, :ref])
    }
  end

  defp do_attach_telemetry(pid) do
    :ok =
      :telemetry.attach_many(
        "telemetry-broadcaster",
        [
          [:alf, :component, :start],
          [:alf, :component, :stop],
          [:alf, :component, :exception]
        ],
        &Handler.handle_event/4,
        %{pid: pid}
      )
  end
end
