defmodule ALF.Manager do
  use GenServer

  defstruct name: nil,
            pipeline_module: nil,
            pid: nil,
            pipeline: nil,
            components: [],
            pipeline_sup_pid: nil,
            sup_pid: nil,
            registry: %{}

  use ALF.Manager.StreamTo
  use ALF.Manager.GraphEdges

  alias ALF.Components.Goto
  alias ALF.{Builder, IP, PipelineDynamicSupervisor, Pipeline}

  def start_link(%__MODULE__{} = state) do
    GenServer.start_link(__MODULE__, state, name: state.name)
  end

  def init(%__MODULE__{} = state) do
    state = %{state | pid: self()}

    {:ok, state, {:continue, :init_pipeline}}
  end

  def start(module) when is_atom(module) do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)

    case DynamicSupervisor.start_child(
           sup_pid,
           {__MODULE__, %__MODULE__{sup_pid: sup_pid, name: module, pipeline_module: module}}
         ) do
      {:ok, _manager_pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  def stop(module) when is_atom(module) do
    GenServer.call(module, :stop)
  end

  def terminate(:normal, state) do
    GenServer.stop(state.pipeline_sup_pid)
  end

  def __state__(name_or_pid) when is_atom(name_or_pid) or is_pid(name_or_pid) do
    GenServer.call(name_or_pid, :__state__)
  end

  def handle_continue(:init_pipeline, %__MODULE__{} = state) do
    {:noreply, start_pipeline(state)}
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
      case DynamicSupervisor.start_child(
             state.sup_pid,
             {PipelineDynamicSupervisor, %{name: :"#{state.name}_DynamicSupervisor"}}
           ) do
        {:ok, pid} -> pid
        {:error, {:already_started, pid}} -> pid
      end

    Process.monitor(pipeline_sup_pid)
    %{state | pipeline_sup_pid: pipeline_sup_pid}
  end

  defp build_pipeline(%__MODULE__{} = state) do
    {:ok, pipeline} = Builder.build(state.pipeline_module.alf_components, state.pipeline_sup_pid)
    %{state | pipeline: pipeline}
  end

  defp save_stages_states(%__MODULE__{} = state) do
    components =
      state.pipeline.components
      |> Pipeline.stages_to_list()
      |> Enum.map(fn stage ->
        stage.__struct__.__state__(stage.pid)
      end)

    components = [state.pipeline.producer | components] ++ [state.pipeline.consumer]
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
  def handle_call(:stop, _from, state), do: {:stop, :normal, state, state}

  def handle_info({:DOWN, _ref, :process, _pid, :shutdown}, %__MODULE__{} = state) do
    state =
      state
      |> start_pipeline()
      |> resend_packets()

    {:noreply, state}
  end
end
