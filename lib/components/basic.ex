defmodule ALF.Components.Basic do
  alias ALF.{ErrorIP, Manager, IP}

  @common_attributes [
    name: nil,
    pid: nil,
    pipeline_module: nil,
    subscribed_to: [],
    subscribers: [],
    stage_set_ref: nil,
    telemetry: false
  ]

  def common_attributes, do: @common_attributes

  def build_component(component_module, atom, name, opts, current_module) do
    name = if name, do: name, else: atom

    if module_exist?(atom) do
      struct(component_module, %{
        pipeline_module: current_module,
        name: name,
        module: atom,
        function: :call,
        opts: opts || []
      })
    else
      struct(component_module, %{
        pipeline_module: current_module,
        name: name,
        module: current_module,
        function: atom,
        opts: opts || []
      })
    end
  end

  def build_error_ip(ip, error, stacktrace, state) do
    %ErrorIP{
      ip: ip,
      pipeline_module: ip.pipeline_module,
      destination: ip.destination,
      ref: ip.ref,
      stream_ref: ip.stream_ref,
      error: error,
      debug: ip.debug,
      history: ip.history,
      stacktrace: stacktrace,
      component: state,
      plugs: ip.plugs
    }
  end

  def __state__(pid) when is_pid(pid) do
    GenStage.call(pid, :__state__)
  end

  def telemetry_data(nil, state) do
    %{ip: nil, component: component_telemetry_data(state)}
  end

  def telemetry_data(ips, state) when is_list(ips) do
    %{ips: Enum.map(ips, &ip_telemetry_data/1), component: component_telemetry_data(state)}
  end

  def telemetry_data(ip, state) do
    %{ip: ip_telemetry_data(ip), component: component_telemetry_data(state)}
  end

  defp ip_telemetry_data(ip) do
    Map.take(ip, [:type, :event, :ref, :error, :stacktrace])
  end

  defp component_telemetry_data(state) do
    Map.take(state, [:pid, :name, :number, :pipeline_module, :type, :stage_set_ref])
  end

  defp module_exist?(module) do
    case Code.ensure_compiled(module) do
      {:module, ^module} ->
        true

      {:error, _} ->
        false
    end
  end

  defmacro __using__(_opts) do
    quote do
      use GenStage

      alias ALF.{Components.Basic, IP, ErrorIP, Manager.Streamer, SourceCode}

      @type t :: %__MODULE__{}

      def __state__(pid) when is_pid(pid), do: Basic.__state__(pid)

      def subscribers(pid) do
        GenStage.call(pid, :subscribers)
      end

      def init_opts(module, opts) do
        if function_exported?(module, :init, 1) do
          apply(module, :init, [opts])
        else
          opts
        end
      end

      @type result :: :cloned | :destroyed | :created_decomposer | :created_recomposer

      @spec send_result(IP.t() | ErrorIP.t(), result | IP.t() | ErrorIP.t()) ::
              IP.t() | ErrorIP.t()
      def send_result(ip, result) do
        ref = if ip.stream_ref, do: ip.stream_ref, else: ip.ref
        if ip.destination, do: send(ip.destination, {ref, result})
        ip
      end

      @spec send_error_result(IP.t(), any, list, __MODULE__.t()) :: ErrorIP.t()
      def send_error_result(ip, error, stacktrace, state) do
        error_ip = build_error_ip(ip, error, stacktrace, state)
        send_result(error_ip, error_ip)
      end

      @impl true
      def handle_call(:subscribers, _form, state) do
        {:reply, state.subscribers, [], state}
      end

      def handle_call(:__state__, _from, state) do
        {:reply, state, [], state}
      end

      @impl true
      def handle_subscribe(:consumer, subscription_options, from, state) do
        subscribers = [{from, subscription_options} | state.subscribers]
        new_state = %{state | subscribers: subscribers}
        Manager.component_updated(state.pipeline_module, new_state)
        {:automatic, new_state}
      end

      def handle_subscribe(:producer, subscription_options, from, state) do
        subscribed_to = [{from, subscription_options} | state.subscribed_to]
        new_state = %{state | subscribed_to: subscribed_to}
        Manager.component_updated(state.pipeline_module, new_state)
        {:automatic, new_state}
      end

      @impl true
      def handle_cancel(_any, from, state) do
        subscribed_to = Enum.filter(state.subscribed_to, &(&1 != from))
        subscribers = Enum.filter(state.subscribers, &(&1 != from))
        state = %{state | subscribed_to: subscribed_to, subscribers: subscribers}
        Manager.component_updated(state.pipeline_module, state)
        {:noreply, [], state}
      end

      def telemetry_data(ip, state), do: ALF.Components.Basic.telemetry_data(ip, state)

      def read_source_code(module) do
        SourceCode.module_source(module)
      rescue
        error ->
          inspect(error)
      end

      def read_source_code(module, :call) do
        SourceCode.module_source(module)
      rescue
        error ->
          inspect(error)
      end

      def read_source_code(module, function) do
        do_read_source_code(module, function)
      rescue
        error ->
          inspect(error)
      end

      def component_added(component) do
        Manager.component_added(component.pipeline_module, component)
      end

      def history(ip, state, include_number \\ false) do
        name = if include_number, do: {state.name, state.number}, else: state.name

        if ip.debug do
          [{name, ip.event} | ip.history]
        else
          []
        end
      end

      defp do_read_source_code(module, function) do
        doc = SourceCode.function_doc(module, function)
        source = SourceCode.function_source(module, function)

        if doc do
          "@doc \"#{doc}\"\n#{source}"
        else
          source
        end
      end

      def build_error_ip(ip, error, stacktrace, state) do
        ALF.Components.Basic.build_error_ip(ip, error, stacktrace, state)
      end
    end
  end
end
