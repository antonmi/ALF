defmodule ALF.Manager do
  use GenServer

  defstruct name: nil,
            pipeline_module: nil,
            pid: nil,
            pipeline: nil,
            components: [],
            pipeline_sup_pid: nil,
            sup_pid: nil,
            producer_pid: nil,
            registry: %{},
            registry_dump: %{}

  alias ALF.AutoScaler
  alias ALF.Manager.{Components, Streamer, ProcessingOptions, StreamRegistry}
  alias ALF.Components.{Goto, Producer}
  alias ALF.{Builder, Introspection, PipelineDynamicSupervisor, Pipeline}

  @max_producer_load 100
  def max_producer_load, do: @max_producer_load

  def start_link(%__MODULE__{} = state) do
    GenServer.start_link(__MODULE__, state, name: state.name)
  end

  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    {:ok, state, {:continue, :init_pipeline}}
  end

  @spec start(atom, atom) :: :ok
  def start(module, name \\ nil) when is_atom(module) and is_atom(name) do
    unless is_pipeline_module?(module) do
      raise "The #{module} doesn't implement any pipeline"
    end

    sup_pid = Process.whereis(ALF.DynamicSupervisor)

    name = if name, do: name, else: module

    case DynamicSupervisor.start_child(
           sup_pid,
           %{
             id: __MODULE__,
             start:
               {__MODULE__, :start_link,
                [%__MODULE__{sup_pid: sup_pid, name: name, pipeline_module: module}]},
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
    result = GenServer.call(module, :stop, :infinity)
    Introspection.remove(module)
    result
  catch
    :exit, {reason, details} ->
      {:exit, {reason, details}}
  end

  @spec stream_to(Enumerable.t(), atom(), map() | keyword()) :: Enumerable.t()
  def stream_to(stream, name, opts \\ %{}) when is_atom(name) do
    GenServer.call(name, {:stream_to, stream, ProcessingOptions.new(opts), false})
  end

  @spec steam_with_ids_to(Enumerable.t({term, term}), atom(), map() | keyword()) ::
          Enumerable.t()
  def steam_with_ids_to(stream, name, opts \\ %{}) when is_atom(name) do
    GenServer.call(name, {:stream_to, stream, ProcessingOptions.new(opts), true})
  end

  @spec components(atom) :: list(map())
  def components(name) when is_atom(name) do
    GenServer.call(name, :components)
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

  def terminate(:normal, state) do
    Supervisor.stop(state.pipeline_sup_pid)
  end

  def __state__(name_or_pid) when is_atom(name_or_pid) or is_pid(name_or_pid) do
    GenServer.call(name_or_pid, :__state__)
  end

  def __set_state__(name_or_pid, new_state) when is_atom(name_or_pid) or is_pid(name_or_pid) do
    GenServer.call(name_or_pid, {:__set_state__, new_state})
  end

  def handle_continue(:init_pipeline, %__MODULE__{} = state) do
    {:noreply, start_pipeline(state), {:continue, :register_auto_scaling}}
  end

  def handle_continue(:register_auto_scaling, %__MODULE__{} = state) do
    if telemetry_enabled?() do
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
        state.pipeline_module.alf_components,
        state.pipeline_sup_pid,
        state.name,
        state.pipeline_module
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

  def handle_call({:stream_to, stream, opts, custom_ids?}, _from, %__MODULE__{} = state) do
    {stream, state} = Streamer.prepare_streams(state, stream, opts, custom_ids?)
    {:reply, stream, state}
  end

  def handle_call({:flush_queue, stream_ref}, _from, state) do
    registry = state.registry[stream_ref]

    if registry do
      events =
        case :queue.to_list(registry.queue) do
          [] ->
            if StreamRegistry.empty?(registry), do: :done, else: {:ok, []}

          events when is_list(events) ->
            {:ok, events}
        end

      new_registry = Map.put(state.registry, stream_ref, %{registry | queue: :queue.new()})

      {:reply, events, %{state | registry: new_registry}}
    else
      {:reply, {:ok, []}, state}
    end
  end

  def handle_call(:components, _from, state) do
    {:reply, state.components, state}
  end

  def handle_call(:producer_ips_count, _from, state) do
    count = Producer.ips_count(state.producer_pid)
    {:reply, count, state}
  end

  def handle_call({:add_component, stage_set_ref}, _from, state) do
    existing_workers = Components.find_existing_workers(state.components, stage_set_ref)

    new_stage = Builder.add_stage_worker(state.pipeline_sup_pid, existing_workers)

    stages_to_subscribe_pids = Enum.map(new_stage.subscribers, fn {pid, _opts} -> pid end)
    subscribe_to_pids = Enum.map(new_stage.subscribe_to, fn {pid, _opts} -> pid end)

    new_components =
      Components.refresh_components_after_adding(
        state.components,
        new_stage,
        stages_to_subscribe_pids,
        subscribe_to_pids
      )

    {:reply, new_stage, %{state | components: new_components}}
  end

  def handle_call({:remove_component, stage_set_ref}, _from, state) do
    existing_workers = Components.find_existing_workers(state.components, stage_set_ref)

    if length(existing_workers) > 1 do
      stage_to_delete = Enum.max_by(existing_workers, & &1.number)

      new_components =
        Components.refresh_components_before_removing(state.components, stage_to_delete)

        # TODO do it differently
#        Task.async(fn ->
#          Process.sleep(1000)
#          :ok = Builder.delete_stage_worker(state.pipeline_sup_pid, stage_to_delete)
#        end)

      {:reply, stage_to_delete, %{state | components: new_components}}
    else
      {:reply, {:error, :only_one_left}, state}
    end
  end

  def handle_cast({:add_to_registry, ips, stream_ref}, state) do
    new_registry = Streamer.add_to_registry(state.registry, stream_ref, ips)
    {:noreply, %{state | registry: new_registry}}
  end

  def handle_cast({:remove_from_registry, ips, stream_ref}, state) do
    new_registry = Streamer.remove_from_registry(state.registry, stream_ref, ips)
    {:noreply, %{state | registry: new_registry}}
  end

  def handle_cast({:result_ready, ip}, state) do
    new_registry = Streamer.rebuild_registry_on_result_ready(state.registry, ip)
    {:noreply, %{state | registry: new_registry}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, :shutdown}, %__MODULE__{} = state) do
    state =
      state
      |> start_pipeline()
      |> copy_registry_to_dump()
      |> Streamer.resend_packets()

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

  defp telemetry_enabled? do
    Application.get_env(:alf, :telemetry_enabled, false)
  end
end
