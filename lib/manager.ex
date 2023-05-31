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
            telemetry_enabled: nil,
            sync: false

  alias ALF.Components.{Goto, Producer}
  alias ALF.{Builder, Introspection, PipelineDynamicSupervisor, Pipeline, SyncRunner}
  alias ALF.{ErrorIP, IP}

  @type t :: %__MODULE__{}

  @available_options [:telemetry_enabled, :sync]
  @default_timeout 60_000

  @spec start_link(t()) :: GenServer.on_start()
  def start_link(%__MODULE__{} = state) do
    GenServer.start_link(__MODULE__, state, name: state.name)
  end

  @impl true
  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    if state.sync do
      pipeline = Builder.build_sync(state.pipeline_module, state.telemetry_enabled)
      {:ok, %{state | pipeline: pipeline, components: Pipeline.stages_to_list(pipeline)}}
    else
      {:ok, start_pipeline(state)}
    end
  end

  @spec start(atom) :: :ok
  def start(module) when is_atom(module) do
    start(module, module, [])
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
  def started?(name) when is_atom(name) do
    if Process.whereis(name), do: true, else: false
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
  def call(event, name, opts \\ [return_ip: false]) do
    case check_if_ready(name) do
      {:ok, producer_name} ->
        do_call(name, producer_name, event, opts)

      {:sync, pipeline} ->
        do_sync_call(name, pipeline, event, opts)
    end
  end

  defp do_call(name, producer_name, event, opts) do
    ip = build_ip(event, name)
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

  defp do_sync_call(name, pipeline, event, opts) do
    ip = build_ip(event, name)

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
  def cast(event, name, opts \\ [send_result: false]) do
    case check_if_ready(name) do
      {:ok, producer_name} ->
        do_cast(name, producer_name, event, opts)

      {:sync, _pipeline} ->
        raise "Not implemented"
    end
  end

  defp do_cast(name, producer_name, event, opts) do
    ip =
      case opts[:send_result] do
        true ->
          build_ip(event, name)

        false ->
          %{build_ip(event, name) | destination: false}
      end

    Producer.load_ip(producer_name, ip)
    ip.ref
  end

  @spec stream(Enumerable.t(), atom, Keyword.t()) :: Enumerable.t()
  def stream(stream, name, opts \\ [return_ip: false]) do
    case check_if_ready(name) do
      {:ok, producer_name} ->
        do_stream(name, producer_name, stream, opts)

      {:sync, pipeline} ->
        do_sync_stream(name, pipeline, stream, opts)
    end
  end

  defp do_stream(name, producer_name, stream, opts) do
    stream_ref = make_ref()
    timeout = opts[:timeout] || @default_timeout

    stream
    |> Stream.transform(
      nil,
      fn event, nil ->
        ip = build_ip(event, name)
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

  defp do_sync_stream(name, pipeline, stream, opts) do
    stream
    |> Stream.transform(
      nil,
      fn event, nil ->
        ip = build_ip(event, name)
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
  def components(name) when is_atom(name) do
    GenServer.call(name, :components)
  end

  @spec reload_components_states(atom()) :: list(map())
  def reload_components_states(name) when is_atom(name) do
    GenServer.call(name, :reload_components_states)
  end

  @impl true
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

  @impl true
  def handle_call(:__state__, _from, state), do: {:reply, state, state}

  def handle_call({:__set_state__, new_state}, _from, _state) do
    {:reply, new_state, new_state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, state, state}
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

  def handle_call(:sync_pipeline, _from, state) do
    if state.sync do
      {:reply, state.pipeline, state}
    else
      raise "#{state.name} is not a sync pipeline"
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, :shutdown}, %__MODULE__{} = state) do
    state = start_pipeline(state)

    {:noreply, state}
  end

  defp is_pipeline_module?(module) when is_atom(module) do
    is_list(module.alf_components())
  rescue
    _error -> false
  end

  defp telemetry_enabled_in_configs? do
    Application.get_env(:alf, :telemetry_enabled, false)
  end

  defp check_if_ready(name) do
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
      init_event: event,
      event: event,
      manager_name: name
    }
  end
end
