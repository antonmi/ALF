defmodule ALF.Components.Plug do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :plug,
                module: nil,
                opts: [],
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
        source_code: read_source_code(state.module, :plug)
    }

    {:producer_consumer, state, subscribe_to: state.subscribe_to}
  end

  def handle_events([%IP{} = ip], _from, %__MODULE__{telemetry_enabled: true} = state) do
    :telemetry.span(
      [:alf, :component],
      telemetry_data(ip, state),
      fn ->
        case process_ip(ip, state) do
          %IP{} = ip ->
            {{:noreply, [ip], state}, telemetry_data(ip, state)}

          %ErrorIP{} = ip ->
            {{:noreply, [], state}, telemetry_data(ip, state)}
        end
      end
    )
  end

  def handle_events([%IP{} = ip], _from, %__MODULE__{telemetry_enabled: false} = state) do
    case process_ip(ip, state) do
      %IP{} = ip ->
        {:noreply, [ip], state}

      %ErrorIP{} ->
        {:noreply, [], state}
    end
  end

  defp process_ip(ip, state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}
    ip_plugs = Map.put(ip.plugs, state.name, ip.event)
    ip = %{ip | plugs: ip_plugs}

    case call_plug_function(state.module, ip.event, state.opts) do
      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)

      new_datum ->
        %{ip | event: new_datum}
    end
  end

  def sync_process(ip, state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}
    ip_plugs = Map.put(ip.plugs, state.name, ip.event)
    ip = %{ip | plugs: ip_plugs}

    case call_plug_function(state.module, ip.event, state.opts) do
      {:error, error, stacktrace} ->
        build_error_ip(ip, error, stacktrace, state)

      new_datum ->
        %{ip | event: new_datum}
    end
  end

  defp call_plug_function(module, event, opts) do
    apply(module, :plug, [event, opts])
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end
end
