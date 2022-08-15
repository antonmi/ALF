defmodule ALF.Components.Unplug do
  use ALF.Components.Basic

  defstruct Basic.common_attributes() ++
              [
                type: :unplug,
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
        source_code: read_source_code(state.module, :unplug)
    }

    {:producer_consumer, state, subscribe_to: state.subscribe_to}
  end

  def init_sync(state, telemetry_enabled) do
    %{
      state
      | pid: make_ref(),
        opts: init_opts(state.module, state.opts),
        source_code: read_source_code(state.module, :unplug),
        telemetry_enabled: telemetry_enabled
    }
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

    prev_event = Map.fetch!(ip.plugs, state.name)
    ip_plugs = Map.delete(ip.plugs, state.name)
    ip = %{ip | plugs: ip_plugs}

    case call_unplug_function(state.module, ip.event, prev_event, state.opts) do
      {:error, error, stacktrace} ->
        send_error_result(ip, error, stacktrace, state)

      new_datum ->
        %{ip | event: new_datum}
    end
  end

  def sync_process(ip, state) do
    ip = %{ip | history: [{state.name, ip.event} | ip.history]}

    prev_event = Map.fetch!(ip.plugs, state.name)
    ip_plugs = Map.delete(ip.plugs, state.name)
    ip = %{ip | plugs: ip_plugs}

    case call_unplug_function(state.module, ip.event, prev_event, state.opts) do
      {:error, error, stacktrace} ->
        build_error_ip(ip, error, stacktrace, state)

      new_datum ->
        %{ip | event: new_datum}
    end
  end

  defp call_unplug_function(module, event, prev_event, opts) do
    apply(module, :unplug, [event, prev_event, opts])
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end
end
