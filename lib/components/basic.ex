defmodule ALF.Components.Basic do
  alias ALF.IP

  def build_component(component_module, atom, name, opts, current_module) do
    name = if name, do: name, else: atom

    if module_exist?(atom) do
      struct(component_module, %{
        pipe_module: current_module,
        pipeline_module: current_module,
        name: name,
        module: atom,
        function: :call,
        opts: opts || []
      })
    else
      struct(component_module, %{
        pipe_module: current_module,
        pipeline_module: current_module,
        name: name,
        module: current_module,
        function: atom,
        opts: opts || []
      })
    end
  end

  def telemetry_enabled? do
    Application.get_env(:alf, :telemetry_enabled, false)
  end

  def telemetry_data(%IP{} = ip, state) do
    %{ip: ip_telemetry_data(ip), component: component_telemetry_data(state)}
  end

  def telemetry_data([%IP{} | _] = ips, state) do
    %{ips: Enum.map(ips, &ip_telemetry_data/1), component: component_telemetry_data(state)}
  end

  def telemetry_data(nil, state) do
    %{ip: nil, component: component_telemetry_data(state)}
  end

  defp ip_telemetry_data(ip), do: Map.take(ip, [:event, :ref])

  defp component_telemetry_data(state) do
    Map.take(state, [:pid, :name, :number, :pipeline_module, :type, :stage_set_ref])
  end

  defp module_exist?(module), do: function_exported?(module, :__info__, 1)

  defmacro __using__(_opts) do
    quote do
      use GenStage

      alias ALF.{IP, ErrorIP, Manager.Streamer, SourceCode}

      def __state__(pid) when is_pid(pid) do
        GenStage.call(pid, :__state__)
      end

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

      def send_error_result(ip, error, stacktrace, state) do
        error_ip = build_error_ip(ip, error, stacktrace, state)
        Streamer.cast_result_ready(error_ip.manager_name, error_ip)
      end

      def add_subscribe_to(state, subscribe_to),
        do: GenStage.call(state.pid, {:add_subscribe_to, subscribe_to})

      def remove_subscribe_to(state, subscribe_to),
        do: GenStage.call(state.pid, {:remove_subscribe_to, subscribe_to})

      def add_subscriber(state, subscriber),
        do: GenStage.call(state.pid, {:add_subscriber, subscriber})

      def remove_subscriber(state, subscriber),
        do: GenStage.call(state.pid, {:remove_subscriber, subscriber})

      def handle_call({:add_subscribe_to, subscribe_to}, _from, state) do
        new_subscribe_to = [subscribe_to | state.subscribe_to]
        state = %{state | subscribe_to: new_subscribe_to}
        {:reply, state, [], state}
      end

      def handle_call({:remove_subscribe_to, subscribe_to}, _from, state) do
        new_subscribe_to = Enum.filter(state.subscribe_to, &(&1 != subscribe_to))
        state = %{state | subscribe_to: new_subscribe_to}
        {:reply, state, [], state}
      end

      def handle_call({:add_subscriber, subscriber}, _from, state) do
        new_subscribers = [subscriber | state.subscribers]
        state = %{state | subscribers: new_subscribers}
        {:reply, state, [], state}
      end

      def handle_call({:remove_subscriber, subscriber}, _from, state) do
        new_subscribers = Enum.filter(state.subscribers, &(&1 != subscriber))
        state = %{state | subscribers: new_subscribers}
        {:reply, state, [], state}
      end

      def handle_call(:subscribers, _form, state) do
        {:reply, state.subscribers, [], state}
      end

      def handle_call(:__state__, _from, state) do
        {:reply, state, [], state}
      end

      def handle_subscribe(:consumer, subscription_options, from, state) do
        subscribers = [from | state.subscribers]
        {:automatic, %{state | subscribers: subscribers}}
      end

      def handle_subscribe(:producer, subscription_options, from, state) do
        {:automatic, state}
      end

      def telemetry_enabled?, do: ALF.Components.Basic.telemetry_enabled?()

      def telemetry_data(ip, state), do: ALF.Components.Basic.telemetry_data(ip, state)

      def read_source_code(module, :call) do
        SourceCode.module_source(module)
      end

      def read_source_code(module, function) do
        SourceCode.function_source(module, function)
      end

      defp build_error_ip(ip, error, stacktrace, state) do
        %ErrorIP{
          ip: ip,
          manager_name: ip.manager_name,
          ref: ip.ref,
          stream_ref: ip.stream_ref,
          error: error,
          stacktrace: stacktrace,
          component: state,
          decomposed: ip.decomposed,
          recomposed: ip.recomposed,
          in_progress: ip.in_progress,
          plugs: ip.plugs
        }
      end
    end
  end
end
