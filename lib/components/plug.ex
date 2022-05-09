defmodule ALF.Components.Plug do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :plug,
                module: nil,
                opts: [],
                doc: nil,
                source_code: nil
              ]

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    state = %{
      state
      | pid: self(),
        opts: init_opts(state.module, state.opts),
        doc: read_doc(state.module, :plug),
        source_code: read_source_code(state.module, :plug)
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
    ip_plugs = Map.put(ip.plugs, state.name, ip.event)
    ip = %{ip | plugs: ip_plugs}

    case call_plug_function(state.module, ip.event, state.opts) do
      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)
        {:noreply, [], state}

      new_datum ->
        {:noreply, [%{ip | event: new_datum}], state}
    end
  end

  defp call_plug_function(module, event, opts) do
    apply(module, :plug, [event, opts])
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end
end
