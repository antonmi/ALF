defmodule ALF.TelemetryBroadcaster do
  use GenServer

  defmodule Handler do
    def handle_event([:alf, :component, type], measurements, metadata, %{pid: pid}) do
      GenServer.cast(pid, {:handle_event, [:alf, :component, type], measurements, metadata})
    end
  end

  defstruct remote_function: nil,
            pid: nil,
            components_with_timestamps: %{}

  def start_link([]) do
    GenServer.start_link(__MODULE__, %__MODULE__{}, name: __MODULE__)
  end

  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    {:ok, state, {:continue, :attach_telemetry}}
  end

  def handle_continue(:attach_telemetry, %__MODULE__{} = state) do
    do_attach_telemetry(state.pid)
    {:noreply, state}
  end

  @spec register_remote_function(atom(), atom(), atom()) :: {atom(), atom(), atom()}
  def register_remote_function(name, module, function, opts \\ %{}) do
    GenServer.call(__MODULE__, {:register_remote_function, {name, module, function, opts}})
  end

  def remote_function(), do: GenServer.call(__MODULE__, :remote_function)

  def handle_call(:remote_function, _from, state), do: {:reply, state.remote_function, state}

  def handle_call({:register_remote_function, {name, module, function, opts}}, _from, state) do
    state = %{state | remote_function: {name, module, function, opts}}
    {:reply, {name, module, function, opts}, state}
  end

  def handle_cast({:handle_event, [:alf, :component, _], _, _}, %{remote_function: nil} = state) do
    {:noreply, state}
  end

  def handle_cast({:handle_event, [:alf, :component, type], measurements, metadata}, state) do
    {name, module, function, opts} = state.remote_function

    if opts[:interval] do
      component_pid = get_in(metadata, [:component, :pid])
      timestamp = Map.get(state.components_with_timestamps, {component_pid, type}, false)
      time_now = Time.utc_now()

      if timestamp do
        if Time.diff(time_now, timestamp, :millisecond) > opts[:interval] do
          :rpc.call(name, module, function, [[:alf, :component, type], measurements, metadata])

          new_components_with_timestamps =
            Map.put(state.components_with_timestamps, {component_pid, type}, time_now)

          new_state = %{state | components_with_timestamps: new_components_with_timestamps}
          {:noreply, new_state}
        else
          {:noreply, state}
        end
      else
        :rpc.call(name, module, function, [[:alf, :component, type], measurements, metadata])

        new_components_with_timestamps =
          Map.put(state.components_with_timestamps, {component_pid, type}, time_now)

        new_state = %{state | components_with_timestamps: new_components_with_timestamps}
        {:noreply, new_state}
      end
    else
      :rpc.call(name, module, function, [[:alf, :component, type], measurements, metadata])
      {:noreply, state}
    end
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
