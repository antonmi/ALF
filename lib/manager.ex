defmodule ALF.Manager do
  use GenServer

  defstruct name: nil,
            pipeline_module: nil,
            pid: nil,
            pipeline: nil,
            components: [],
            stages_to_be_deleted: [],
            pipeline_sup_pid: nil,
            sup_pid: nil,
            producer_pid: nil,
            registry: %{},
            registry_dump: %{},
            autoscaling_enabled: nil,
            telemetry_enabled: nil,
            sync: false

  alias ALF.AutoScaler
  alias ALF.Manager.{Components, Streamer, ProcessingOptions, StreamRegistry, SyncRunner}
  alias ALF.Components.{Goto, Producer}
  alias ALF.{Builder, Introspection, PipelineDynamicSupervisor, Pipeline}
  alias ALF.{ErrorIP, IP}

  @type t :: %__MODULE__{}

  @available_options [:autoscaling_enabled, :telemetry_enabled, :sync]
  @default_timeout 60_000

  @max_producer_load 100
  def max_producer_load, do: @max_producer_load

  def start_link(%__MODULE__{} = state) do
    GenServer.start_link(__MODULE__, state, name: state.name)
  end

  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    if state.sync do
      pipeline =
        Builder.build_sync(
          state.pipeline_module,
          state.telemetry_enabled
        )

      {:ok, %{state | pipeline: pipeline, components: Pipeline.stages_to_list(pipeline)}}
    else
      {:ok, start_pipeline(state), {:continue, :register_auto_scaling}}
    end
  end

  @spec start(atom) :: :ok
  def start(module) when is_atom(module) do
    start(module, module, [])
  end

  @spec start(atom, atom) :: :ok
  def start(module, name) when is_atom(module) and is_atom(name) do
    start(module, name, [])
  end

  @spec start(atom, list) :: :ok
  def start(module, opts) when is_atom(module) and is_list(opts) do
    start(module, module, opts)
  end

  @spec start(atom, atom, list) :: :ok
  def start(module, name, opts) when is_atom(module) and is_atom(name) and is_list(opts) do
    unless is_pipeline_module?(module) do
      raise "The #{module} doesn't implement any pipeline"
    end

    wrong_options = Keyword.keys(opts) -- @available_options

    if Enum.any?(wrong_options) do
      raise "Wrong options for the '#{name}' pipeline: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(@available_options)}"
    end

    sup_pid = Process.whereis(ALF.DynamicSupervisor)

    name = if name, do: name, else: module

    case DynamicSupervisor.start_child(
           sup_pid,
           %{
             id: __MODULE__,
             start:
               {__MODULE__, :start_link,
                [
                  %__MODULE__{
                    sup_pid: sup_pid,
                    name: name,
                    pipeline_module: module,
                    autoscaling_enabled: Keyword.get(opts, :autoscaling_enabled, false),
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

  def stop(module) when is_atom(module) do
    AutoScaler.unregister_pipeline(module)
    result = GenServer.call(module, :stop, :infinity)
    Introspection.remove(module)
    result
  catch
    :exit, {reason, details} ->
      {:exit, {reason, details}}
  end

  @spec stream_to(Enumerable.t(), atom(), map() | keyword()) :: Enumerable.t()
  def stream_to(stream, name, opts \\ []) when is_atom(name) do
    stream(stream, name, opts)
  end

  @spec components(atom) :: list(map())
  def components(name) when is_atom(name) do
    GenServer.call(name, :components)
  end

  @spec reload_components_states(atom()) :: list(map())
  def reload_components_states(name) when is_atom(name) do
    GenServer.call(name, :reload_components_states)
  end

  @spec producer_ips_count(atom) :: integer()
  def producer_ips_count(name) when is_atom(name) do
    GenServer.call(name, :producer_ips_count)
  end

  def add_component(name, stage_set_ref) do
    GenServer.call(name, {:add_component, stage_set_ref})
  end

  def remove_component(name, stage_set_ref) do
    GenServer.call(name, {:remove_component, stage_set_ref})
  end

  def delete_marked_to_be_deleted(name) when is_atom(name) do
    GenServer.call(name, :delete_marked_to_be_deleted)
  end

  def terminate(:normal, state) do
    unless state.sync do
      Supervisor.stop(state.pipeline_sup_pid)
    end
  end

  def __state__(name_or_pid) when is_atom(name_or_pid) or is_pid(name_or_pid) do
    GenServer.call(name_or_pid, :__state__)
  end

  def __set_state__(name_or_pid, new_state) when is_atom(name_or_pid) or is_pid(name_or_pid) do
    GenServer.call(name_or_pid, {:__set_state__, new_state})
  end

  def handle_continue(:register_auto_scaling, %__MODULE__{} = state) do
    if state.autoscaling_enabled do
      AutoScaler.register_pipeline(state.pipeline_module)
    end

    {:noreply, state}
  end

  defp start_pipeline(%__MODULE__{} = state) do
    state
    |> start_pipeline_supervisor()
    |> build_pipeline()
    |> save_stages_states()
    |> prepare_gotos()
  end

  defp start_pipeline_supervisor(%__MODULE__{} = state) do
    pipeline_sup_pid =
      case PipelineDynamicSupervisor.start_link(%{name: :"#{state.name}_DynamicSupervisor"}) do
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
        state.name,
        state.telemetry_enabled
      )

    %{state | pipeline: pipeline, producer_pid: pipeline.producer.pid}
  end

  defp save_stages_states(%__MODULE__{} = state) do
    components =
      [state.pipeline.producer | Pipeline.stages_to_list(state.pipeline.components)] ++
        [state.pipeline.consumer]

    components =
      components
      |> Enum.map(fn stage ->
        stage.__struct__.__state__(stage.pid)
      end)

    %{state | components: components}
  end

  defp prepare_gotos(%__MODULE__{} = state) do
    components =
      state.components
      |> Enum.map(fn component ->
        case component do
          %Goto{} ->
            Goto.find_where_to_go(component.pid, state.components)

          stage ->
            stage
        end
      end)

    %{state | components: components}
  end

  def handle_call(:__state__, _from, state), do: {:reply, state, state}

  def handle_call({:__set_state__, new_state}, _from, _state) do
    {:reply, new_state, new_state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, state, state}
  end

  def handle_call(
        {:stream_to, stream, opts, custom_ids?},
        _from,
        %__MODULE__{sync: false} = state
      ) do
    {stream, state} = Streamer.prepare_streams(state, stream, opts, custom_ids?)
    {:reply, stream, state}
  end

  def handle_call({:stream_to, stream, opts, custom_ids?}, _from, %__MODULE__{sync: true} = state) do
    stream_ref = make_ref()

    stream =
      SyncRunner.transform_sync_stream(
        {stream, stream_ref},
        {state.name, state.pipeline},
        {opts.return_ips, custom_ids?}
      )

    {:reply, stream, state}
  end

  def handle_call({:flush_queue, stream_ref}, _from, state) do
    registry = state.registry[stream_ref]

    if registry do
      events = :queue.to_list(registry.queue)

      response =
        if StreamRegistry.empty?(registry) do
          {:done, events}
        else
          {:not_done_yet, events}
        end

      new_registry = Map.put(state.registry, stream_ref, %{registry | queue: :queue.new()})

      {:reply, response, %{state | registry: new_registry}}
    else
      {:reply, {:done, []}, state}
    end
  end

  def handle_call(:components, _from, state) do
    {:reply, state.components, state}
  end

  def handle_call(:reload_components_states, _from, %__MODULE__{sync: false} = state) do
    components =
      state.components
      |> Enum.map(fn stage ->
        stage.__struct__.__state__(stage.pid)
      end)

    {:reply, components, %{state | components: components}}
  end

  def handle_call(:reload_components_states, _from, %__MODULE__{sync: true} = state) do
    {:reply, state.components, state}
  end

  def handle_call(:producer_ips_count, _from, state) do
    count = Producer.ips_count(state.producer_pid)
    {:reply, count, state}
  end

  def handle_call({:add_component, stage_set_ref}, _from, state) do
    {new_stage, new_components} =
      Components.add_component(state.components, stage_set_ref, state.pipeline_sup_pid)

    {:reply, new_stage, %{state | components: new_components}}
  end

  def handle_call({:remove_component, stage_set_ref}, _from, state) do
    case Components.remove_component(state.components, stage_set_ref) do
      {:ok, {stage_to_delete, new_components}} ->
        stages_to_be_deleted = [stage_to_delete | state.stages_to_be_deleted]

        {:reply, stage_to_delete,
         %{state | components: new_components, stages_to_be_deleted: stages_to_be_deleted}}

      {:error, :only_one_left} ->
        {:reply, {:error, :only_one_left}, state}
    end
  end

  def handle_call(:delete_marked_to_be_deleted, _from, state) do
    state.stages_to_be_deleted
    |> Enum.each(fn stage ->
      DynamicSupervisor.terminate_child(state.pipeline_sup_pid, stage.pid)
    end)

    {:reply, state.stages_to_be_deleted, state}
  end

  def handle_call({:add_to_registry, ips, stream_ref}, _from, state) do
    new_registry = Streamer.add_to_registry(state.registry, stream_ref, ips)
    {:reply, new_registry, %{state | registry: new_registry}}
  end

  def handle_call({:remove_from_registry, ips, stream_ref}, _from, state) do
    new_registry = Streamer.remove_from_registry(state.registry, stream_ref, ips)
    {:reply, new_registry, %{state | registry: new_registry}}
  end

  def handle_call(:sync_pipeline, _from, state) do
    if state.sync do
      {:reply, state.pipeline, state}
    else
      raise "#{state.name} is not a sync pipeline"
    end
  end

  def handle_cast({:move_to_in_progress_registry, ips, stream_ref}, state) do
    new_registry = Streamer.move_to_in_progress_registry(state.registry, stream_ref, ips)
    {:noreply, %{state | registry: new_registry}}
  end

  def handle_cast({:result_ready, ip}, state) do
    new_registry = Streamer.rebuild_registry_on_result_ready(state.registry, ip)
    {:noreply, %{state | registry: new_registry}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, :shutdown}, %__MODULE__{} = state) do
    state = start_pipeline(state)

    {:noreply, state}
  end

  defp copy_registry_to_dump(state) do
    %{state | registry_dump: state.registry}
  end

  defp is_pipeline_module?(module) when is_atom(module) do
    is_list(module.alf_components())
  rescue
    _error -> false
  end

  defp telemetry_enabled_in_configs? do
    Application.get_env(:alf, :telemetry_enabled, false)
  end

  def call(event, name, opts \\ [return_ip: false]) do
    case status(name) do
      {:ok, producer_name} ->
        do_call(name, producer_name, event, opts)

      {:sync, pipeline} ->
        do_sync_call(name, pipeline, event, opts)
    end
  end

  defp do_call(name, producer_name, event, opts) do
    ip = build_ip(event, name)
    GenServer.cast(producer_name, {:load_ip, ip})

    ref = ip.ref
    timeout = opts[:timeout] || @default_timeout

    receive do
      {^ref, ip} ->
        format_ip(ip, opts[:return_ip])
    after
      timeout ->
        ALF.Components.Basic.build_error_ip(ip, :timeout, [], :no_info)
    end
  end

  defp do_sync_call(name, pipeline, event, opts) do
    ip = build_ip(event, name)
    [ip] = SyncRunner.run(pipeline, ip)
    format_ip(ip, opts[:return_ip])
  end

  def handle_cast({:load_ip, ip}, state) do
    Producer.load_ip(state.producer_pid, ip)

    {:noreply, state}
  end

  def stream(stream, name, opts \\ [return_ips: false]) do
    case status(name) do
      {:ok, producer_name} ->
        do_stream(name, producer_name, stream, opts)

      {:sync, pipeline} ->
        do_sync_stream(name, pipeline, stream, opts)
    end
  end

  defp do_stream(name, producer_name, stream, opts) do
    new_stream_ref = make_ref()
    timeout = opts[:timeout] || @default_timeout

    stream
    |> Stream.transform(
      nil,
      fn event, nil ->
        ip = build_ip(event, name)
        ip = %{ip | new_stream_ref: new_stream_ref}
        GenServer.cast(producer_name, {:load_ip, ip})

        case wait_result(new_stream_ref, [], {timeout, ip}) do
          [] ->
            {[], nil}

          ips ->
            ips = Enum.map(ips, fn ip -> format_ip(ip, opts[:return_ips]) end)
            {ips, nil}
        end
      end
    )
  end

  defp do_sync_stream(name, pipeline, stream, opts) do
    stream
    |> Stream.transform(
      nil,
      fn event, nil ->
        ip = build_ip(event, name)
        ips = SyncRunner.run(pipeline, ip)
        ips = Enum.map(ips, fn ip -> format_ip(ip, opts[:return_ips]) end)
        {ips, nil}
      end
    )
  end

  defp wait_result(new_stream_ref, acc, {timeout, initial_ip}) do
    receive do
      {^new_stream_ref, :created_recomposer} ->
        wait_result(new_stream_ref, acc, {timeout, initial_ip})

      {^new_stream_ref, reason} when reason in [:created_decomposer, :cloned] ->
        wait_result(
          new_stream_ref,
          acc ++ wait_result(new_stream_ref, [], {timeout, initial_ip}),
          {timeout, initial_ip}
        )

      {^new_stream_ref, :destroyed} ->
        acc

      {^new_stream_ref, ip} ->
        Enum.reverse([ip | acc])
    after
      timeout ->
        error_ip = ALF.Components.Basic.build_error_ip(initial_ip, :timeout, [], :no_info)
        Enum.reverse([error_ip | acc])
    end
  end

  defp status(name) do
    producer_name = :"#{name}.Producer"

    cond do
      Process.whereis(producer_name) && Process.whereis(name) ->
        {:ok, producer_name}

      is_nil(Process.whereis(producer_name)) && Process.whereis(name) ->
        {:sync, GenServer.call(name, :sync_pipeline)}

      true ->
        raise("Pipeline #{name} is not started")
    end
  end

  defp format_ip(%IP{} = ip, true), do: ip
  defp format_ip(%IP{} = ip, false), do: ip.event
  defp format_ip(%IP{} = ip, nil), do: ip.event
  defp format_ip(%ErrorIP{} = ip, _return_ips), do: ip

  defp build_ip(event, name) do
    %IP{
      ref: make_ref(),
      destination: self(),
      init_datum: event,
      event: event,
      manager_name: name
    }
  end
end
