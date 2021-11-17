defmodule ALF.Components.Plug do
  use ALF.Components.Basic

  defstruct name: nil,
            module: nil,
            opts: [],
            pipe_module: nil,
            pipeline_module: nil,
            pid: nil,
            subscribe_to: [],
            subscribers: []

  alias ALF.Components.GotoPoint

  alias ALF.DSLError

  def start_link(%__MODULE__{} = state) do
    GenStage.start_link(__MODULE__, state)
  end

  def init(state) do
    state = %{state | pid: self(), opts: init_opts(state.module, state.opts)}
    {:producer_consumer, state, subscribe_to: state.subscribe_to}
  end

  def init_opts(module, opts) do
    if function_exported?(module, :init, 1) do
      apply(module, :init, [opts])
    else
      opts
    end
  end

  def handle_events([%ALF.IP{} = ip], _from, %__MODULE__{} = state) do
    ip = %{ip | history: [{state.name, ip.datum} | ip.history]}
    ip_plugs = Map.put(ip.plugs, state.name, ip.datum)
    ip = %{ip | plugs: ip_plugs}

    case call_plug_function(state.module, ip.datum, state.opts) do
      {:error, error, stacktrace} ->
        {:noreply, [build_error_ip(ip, error, stacktrace, state)], state}

      new_datum ->
        {:noreply, [%{ip | datum: new_datum}], state}
    end
  end

  defp call_plug_function(module, datum, opts) do
    apply(module, :plug, [datum, opts])
  rescue
    error ->
      {:error, error, __STACKTRACE__}
  end
end
