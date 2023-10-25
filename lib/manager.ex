defmodule ALF.Manager do
  use GenServer

  defstruct pipeline_module: nil,
            pid: nil,
            pipeline: nil,
            stages: %{},
            removed_stages: %{},
            pipeline_sup_pid: nil,
            sup_pid: nil,
            producer_pid: nil,
            telemetry_enabled: nil,
            sync: false

  alias ALF.Components.{Consumer, Goto, GotoPoint, Producer}
  alias ALF.{Builder, Introspection, PipelineDynamicSupervisor, Pipeline, SyncRunner}
  alias ALF.{ErrorIP, IP}

  require Logger

  @type t :: %__MODULE__{}

  @available_options [:telemetry_enabled, :sync]
  @default_timeout 10_000

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    GenServer.start_link(__MODULE__, state, name: state.pipeline_module)
  end

  @impl true
  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    if state.sync do
      {:ok, start_sync_pipeline(state)}
    else
      {:ok, start_pipeline(state)}
    end
  end

  defp start_sync_pipeline(state) do
    pipeline = Builder.build_sync(state.pipeline_module, state.telemetry_enabled)

    stages =
      pipeline
      |> Pipeline.stages_to_list()
      |> Enum.reduce(%{}, &Map.put(&2, &1.pid, &1))

    %{state | pipeline: pipeline, stages: stages}
  end

  @spec start(atom) :: :ok
  def start(module) when is_atom(module) do
    start(module, [])
  end

  @spec start(atom, list) :: :ok
  def start(module, opts) when is_atom(module) and is_list(opts) do
    unless is_pipeline_module?(module) do
      raise "The #{module} doesn't implement any pipeline"
    end

    wrong_options = Keyword.keys(opts) -- @available_options

    if Enum.any?(wrong_options) do
      raise "Wrong options for the '#{module}' pipeline: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@available_options)}"
    end

    sup_pid = Process.whereis(ALF.DynamicSupervisor)

    case DynamicSupervisor.start_child(
           sup_pid,
           %{
             id: __MODULE__,
             start:
               {__MODULE__, :start_link,
                [
                  %__MODULE__{
                    sup_pid: sup_pid,
                    pipeline_module: module,
                    telemetry_enabled:
                      Keyword.get(opts, :telemetry_enabled, nil) ||
                        telemetry_enabled_in_configs?(),
                    sync: Keyword.get(opts, :sync, false)
                  }
                ]},
             restart: :transient
           }
         ) do
      {:ok, _manager_pid} ->
        Introspection.add(module)
        :ok

      {:error, {:already_started, _pid}} ->
        :ok
    end
  end

  @spec started?(atom()) :: true | false
  def started?(pipeline_module) when is_atom(pipeline_module) do
    if Process.whereis(pipeline_module), do: true, else: false
  end

  @spec stop(atom) :: :ok | {:exit, {atom, any}}
  def stop(module) when is_atom(module) do
    result = GenServer.call(module, :stop, :infinity)
    Introspection.remove(module)
    result
  catch
    :exit, {reason, details} ->
      {:exit, {reason, details}}
  end

  @spec call(any, atom, Keyword.t()) :: any | [any] | nil
  def call(event, pipeline_module, opts \\ [return_ip: false]) do
    case check_if_ready(pipeline_module) do
      {:ok, producer_name} ->
        do_call(pipeline_module, producer_name, event, opts)

      {:sync, pipeline} ->
        do_sync_call(pipeline_module, pipeline, event, opts)
    end
  end

  defp do_call(pipeline_module, producer_name, event, opts) do
    ip = build_ip(event, pipeline_module)
    Producer.load_ip(producer_name, ip)
    timeout = opts[:timeout] || @default_timeout

    case wait_result(ip.ref, [], {timeout, ip}) do
      [] ->
        nil

      [ip] ->
        format_ip(ip, opts[:return_ip])

      ips ->
        Enum.map(ips, fn ip -> format_ip(ip, opts[:return_ip]) end)
    end
  end

  defp do_sync_call(pipeline_module, pipeline, event, opts) do
    ip = build_ip(event, pipeline_module)

    case SyncRunner.run(pipeline, ip) do
      [] ->
        nil

      [ip] ->
        format_ip(ip, opts[:return_ip])

      ips ->
        Enum.map(ips, fn ip -> format_ip(ip, opts[:return_ip]) end)
    end
  end

  @spec cast(any, atom, Keyword.t()) :: reference
  def cast(event, pipeline_module, opts \\ [send_result: false]) do
    case check_if_ready(pipeline_module) do
      {:ok, producer_name} ->
        do_cast(pipeline_module, producer_name, event, opts)

      {:sync, _pipeline} ->
        raise "Not implemented"
    end
  end

  defp do_cast(pipeline_module, producer_name, event, opts) do
    ip =
      case opts[:send_result] do
        true ->
          build_ip(event, pipeline_module)

        false ->
          %{build_ip(event, pipeline_module) | destination: false}
      end

    Producer.load_ip(producer_name, ip)
    ip.ref
  end

  @spec stream(Enumerable.t(), atom, Keyword.t()) :: Enumerable.t()
  def stream(stream, pipeline_module, opts \\ [return_ip: false]) do
    case check_if_ready(pipeline_module) do
      {:ok, producer_name} ->
        do_stream(pipeline_module, producer_name, stream, opts)

      {:sync, pipeline} ->
        do_sync_stream(pipeline_module, pipeline, stream, opts)
    end
  end

  defp do_stream(pipeline_module, producer_name, stream, opts) do
    stream_ref = make_ref()
    timeout = opts[:timeout] || @default_timeout

    stream
    |> Stream.transform(
      nil,
      fn event, nil ->
        ip = build_ip(event, pipeline_module)
        ip = %{ip | stream_ref: stream_ref}
        Producer.load_ip(producer_name, ip)

        case wait_result(stream_ref, [], {timeout, ip}) do
          [] ->
            {[], nil}

          ips ->
            ips = Enum.map(ips, fn ip -> format_ip(ip, opts[:return_ip]) end)
            {ips, nil}
        end
      end
    )
  end

  defp do_sync_stream(pipeline_module, pipeline, stream, opts) do
    stream
    |> Stream.transform(
      nil,
      fn event, nil ->
        ip = build_ip(event, pipeline_module)
        ips = SyncRunner.run(pipeline, ip)
        ips = Enum.map(ips, fn ip -> format_ip(ip, opts[:return_ip]) end)
        {ips, nil}
      end
    )
  end

  defp wait_result(ref, acc, {timeout, initial_ip}) do
    receive do
      {^ref, :created_recomposer} ->
        wait_result(ref, acc, {timeout, initial_ip})

      {^ref, reason} when reason in [:created_decomposer, :cloned] ->
        wait_result(
          ref,
          acc ++ wait_result(ref, [], {timeout, initial_ip}),
          {timeout, initial_ip}
        )

      {^ref, :destroyed} ->
        acc

      {^ref, ip} ->
        Enum.reverse([ip | acc])
    after
      timeout ->
        error_ip = ALF.Components.Basic.build_error_ip(initial_ip, :timeout, [], :no_info)
        Enum.reverse([error_ip | acc])
    end
  end

  @spec components(atom) :: list(map())
  def components(pipeline_module) when is_atom(pipeline_module) do
    GenServer.call(pipeline_module, :components)
  end

  @spec component_added(atom, map) :: :ok
  def component_added(pipeline_module, component) when is_atom(pipeline_module) do
    GenServer.cast(pipeline_module, {:component_added, component})
  end

  @spec component_updated(atom, map) :: :ok
  def component_updated(pipeline_module, component) when is_atom(pipeline_module) do
    GenServer.cast(pipeline_module, {:component_updated, component})
  end

  @spec reload_components_states(atom()) :: list(map())
  def reload_components_states(pipeline_module) when is_atom(pipeline_module) do
    GenServer.call(pipeline_module, :reload_components_states)
  end

  @impl true
  def terminate(_reason, state) do
    unless state.sync do
      Process.alive?(state.pipeline_sup_pid) and Supervisor.stop(state.pipeline_sup_pid)
    end
  end

  def __state__(name_or_pid) when is_atom(name_or_pid) or is_pid(name_or_pid) do
    GenServer.call(name_or_pid, :__state__)
  end

  def __set_state__(name_or_pid, new_state) when is_atom(name_or_pid) or is_pid(name_or_pid) do
    GenServer.call(name_or_pid, {:__set_state__, new_state})
  end

  defp start_pipeline(%__MODULE__{} = state) do
    state
    |> start_pipeline_supervisor()
    |> build_pipeline()
  end

  defp start_pipeline_supervisor(%__MODULE__{} = state) do
    pipeline_sup_pid =
      case PipelineDynamicSupervisor.start_link(%{
             pipeline_module: :"#{state.pipeline_module}_DynamicSupervisor"
           }) do
        {:ok, pid} -> pid
        {:error, {:already_started, pid}} -> pid
      end

    Process.unlink(pipeline_sup_pid)
    Process.monitor(pipeline_sup_pid)
    %{state | pipeline_sup_pid: pipeline_sup_pid}
  end

  defp build_pipeline(%__MODULE__{} = state) do
    {:ok, pipeline} =
      Builder.build(
        state.pipeline_module,
        state.pipeline_sup_pid,
        state.telemetry_enabled
      )

    %{state | pipeline: pipeline, producer_pid: pipeline.producer.pid}
  end

  defp prepare_gotos(state) do
    state.stages
    |> Enum.each(fn {_pid, component} ->
      case component do
        %Goto{} ->
          goto = Goto.find_where_to_go(component.pid, Map.values(state.stages))
          component_updated(state.pipeline_module, goto)

        _stage ->
          :ok
      end
    end)
  end

  @impl true
  def handle_call(:__state__, _from, state), do: {:reply, state, state}

  def handle_call({:__set_state__, new_state}, _from, _state) do
    {:reply, new_state, new_state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, state, state}
  end

  def handle_call(:components, _from, state) do
    components = Map.values(state.stages)
    {:reply, components, state}
  end

  def handle_call(:stages, _from, state) do
    {:reply, state.stages, state}
  end

  def handle_call(:reload_components_states, _from, %__MODULE__{sync: false} = state) do
    stages =
      Enum.reduce(state.stages, %{}, fn {pid, stage}, acc ->
        Map.put(acc, pid, stage.__struct__.__state__(stage.pid))
      end)

    {:reply, Map.values(stages), %{state | stages: stages}}
  end

  def handle_call(:reload_components_states, _from, %__MODULE__{sync: true} = state) do
    {:reply, Map.values(state.stages), state}
  end

  def handle_call(:sync_pipeline, _from, state) do
    if state.sync do
      {:reply, state.pipeline, state}
    else
      raise "#{state.pipeline_module} is not a sync pipeline"
    end
  end

  @impl true
  def handle_cast({:component_added, component}, state) do
    stages = Map.put(state.stages, component.pid, component)

    removed_stages =
      maybe_resubscribe(state.removed_stages, component.stage_set_ref, component.pid)

    Process.monitor(component.pid)
    state = %{state | stages: stages, removed_stages: removed_stages}

    maybe_prepare_gotos(component, state)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:component_updated, component}, state) do
    stages = Map.put(state.stages, component.pid, component)

    {:noreply, %{state | stages: stages}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, %__MODULE__{} = state) do
    Logger.error(
      "Component #{inspect(pid)} is :DOWN with reason: #{reason} in pipeline: #{state.pipeline_module}"
    )

    if pid == state.pipeline_sup_pid do
      state = start_pipeline(state)
      {:noreply, state}
    else
      case Map.get(state.stages, pid) do
        nil ->
          {:noreply, state}

        component ->
          removed_stages = Map.put(state.removed_stages, component.stage_set_ref, component)

          stages = Map.delete(state.stages, pid)
          {:noreply, %{state | stages: stages, removed_stages: removed_stages}}
      end
    end
  end

  defp maybe_resubscribe(removed_stages, stage_set_ref, component_pid) do
    case Map.get(removed_stages, stage_set_ref) do
      nil ->
        removed_stages

      %{subscribed_to: subscribed_to, subscribers: subscribers} ->
        Enum.each(subscribed_to, fn {{pid, _ref}, opts} ->
          GenStage.async_subscribe(component_pid, Keyword.put(opts, :to, pid))
        end)

        Enum.each(subscribers, fn {{pid, _ref}, opts} ->
          GenStage.async_subscribe(pid, Keyword.put(opts, :to, component_pid))
        end)

        Map.delete(removed_stages, stage_set_ref)
    end
  end

  defp maybe_prepare_gotos(%Consumer{}, state), do: prepare_gotos(state)
  defp maybe_prepare_gotos(%GotoPoint{}, state), do: prepare_gotos(state)
  defp maybe_prepare_gotos(%Goto{}, state), do: prepare_gotos(state)
  defp maybe_prepare_gotos(_other_components, _state), do: :nothing

  defp is_pipeline_module?(module) when is_atom(module) do
    is_list(module.alf_components())
  rescue
    _error -> false
  end

  defp telemetry_enabled_in_configs? do
    Application.get_env(:alf, :telemetry_enabled, false)
  end

  defp check_if_ready(pipeline_module) do
    producer_name = :"#{pipeline_module}.Producer"

    cond do
      Process.whereis(producer_name) && Process.whereis(pipeline_module) ->
        {:ok, producer_name}

      is_nil(Process.whereis(producer_name)) && Process.whereis(pipeline_module) ->
        {:sync, GenServer.call(pipeline_module, :sync_pipeline)}

      true ->
        raise("Pipeline #{pipeline_module} is not started")
    end
  end

  defp format_ip(%IP{} = ip, true), do: ip
  defp format_ip(%IP{} = ip, false), do: ip.event
  defp format_ip(%IP{} = ip, nil), do: ip.event
  defp format_ip(%ErrorIP{} = ip, _return_ips), do: ip

  defp build_ip(event, pipeline_module) do
    %IP{
      ref: make_ref(),
      destination: self(),
      init_event: event,
      event: event,
      pipeline_module: pipeline_module
    }
  end
end
