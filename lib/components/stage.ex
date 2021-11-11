defmodule ALF.Components.Stage do
  use ALF.Components.Basic

  defstruct name: nil,
            count: 1,
            number: 0,
            pipe_module: nil,
            pipeline_module: nil,
            module: nil,
            function: nil,
            opts: %{},
            pid: nil,
            subscribe_to: [],
            subscribers: []

  alias ALF.Manager

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

  def handle_events([%IP{} = ip], _from, %__MODULE__{} = state) do
    {:noreply, [process_ip(ip, state)], state}
  end

  def handle_events([%ErrorIP{} = error_ip], _from, %__MODULE__{} = state) do
    Manager.result_ready(error_ip.manager_name, error_ip)
    {:noreply, [], state}
  end

  defp process_ip(ip, state) do
    ip = %{ip | history: [{{state.name, state.number}, ip.datum} | ip.history]}

    case try_apply(ip.datum, {state.module, state.function, state.opts}) do
      {:ok, new_datum} ->
        %{ip | datum: new_datum}

      {:error, error} ->
        build_error_ip(ip, error, state)
    end
  end

  defp try_apply(datum, {module, function, opts}) do
    new_datum = apply(module, function, [datum, opts])
    {:ok, new_datum}
  rescue
    error ->
      {:error, error}
  end
end
