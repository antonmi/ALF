defmodule ALF.Components.Unplug do
  use ALF.Components.Basic

  defstruct name: nil,
            module: nil,
            opts: [],
            pipe_module: nil,
            pipeline_module: nil,
            pid: nil,
            subscribe_to: [],
            subscribers: [],
            telemetry_enabled: false

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    state = %{
      state
      | pid: self(),
        opts: init_opts(state.module, state.opts),
        telemetry_enabled: telemetry_enabled?()
    }

    {:producer_consumer, state, subscribe_to: state.subscribe_to}
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        case do_handle_event(ip, state) do
          {:noreply, [ip], state} = result ->
            {result, telemetry_data(ip, state)}

          {:noreply, [], state} = result ->
            {result, telemetry_data(nil, state)}
        end
      end
    )
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    do_handle_event(ip, state)
  end

  defp do_handle_event(ip, state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}

    prev_event = Map.fetch!(ip.plugs, state.name)
    ip_plugs = Map.delete(ip.plugs, state.name)
    ip = %{ip | plugs: ip_plugs}

    case call_unplug_function(state.module, ip.event, prev_event, state.opts) do
      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)
        {:noreply, [], state}

      new_datum ->
        {:noreply, [%{ip | event: new_datum}], state}
    end
  end

  defp call_unplug_function(module, event, prev_event, opts) do
    apply(module, :unplug, [event, prev_event, opts])
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end
end
