defmodule ALF.DSL do
  alias ALF.Components.{
    Stage,
    Switch,
    Clone,
    DeadEnd,
    GotoPoint,
    Goto
  }

  defmacro stage(atom, options \\ [opts: [], extra_opts: false, count: 1, name: nil]) do
    count = options[:count]
    opts = options[:opts]
    extra_opts = options[:extra_opts]
    name = options[:name]

    quote do
      build_stage(
        unquote(atom),
        unquote(name),
        unquote(opts),
        unquote(extra_opts),
        unquote(count),
        __MODULE__
      )
    end
  end

  defmacro goto_point(name) do
    quote do
      %GotoPoint{
        name: unquote(name),
        pipe_module: __MODULE__,
        pipeline_module: __MODULE__
      }
    end
  end

  defmacro dead_end(name) do
    quote do
      %DeadEnd{
        name: unquote(name),
        pipe_module: __MODULE__,
        pipeline_module: __MODULE__
      }
    end
  end

  defmacro goto(name, to: to, if: condition) do
    quote do
      %Goto{
        name: unquote(name),
        to: unquote(to),
        if: unquote(condition),
        pipe_module: __MODULE__,
        pipeline_module: __MODULE__
      }
    end
  end

  defmacro stages_from(module, options \\ [opts: %{}, extra_opts: false, count: 1]) do
    count = options[:count]
    opts = options[:opts]
    extra_opts = options[:extra_opts]

    quote do
      unquote(module).alf_components
      |> ALF.DSL.set_pipeline_module(__MODULE__)
      |> ALF.DSL.set_options(unquote(opts), unquote(extra_opts), unquote(count))
    end
  end

  defmacro switch(name, options) do
    quote do
      partitions = ALF.DSL.build_partitions(unquote(options)[:partitions], __MODULE__)

      %Switch{
        name: unquote(name),
        partitions: partitions,
        cond: unquote(options)[:cond],
        pipe_module: __MODULE__,
        pipeline_module: __MODULE__
      }
    end
  end

  defmacro clone(name, options) do
    quote do
      stages = set_pipeline_module(unquote(options)[:to], __MODULE__)

      %Clone{
        name: unquote(name),
        to: stages,
        pipe_module: __MODULE__,
        pipeline_module: __MODULE__
      }
    end
  end

  def build_partitions(partitions, module) do
    partitions
    |> Enum.reduce(%{}, fn {key, stages}, final_specs ->
      stages = set_pipeline_module(stages, module)
      Map.put(final_specs, key, stages)
    end)
  end

  def set_pipeline_module(stages, module) do
    Enum.map(stages, fn stage ->
      %{stage | pipeline_module: module}
    end)
  end

  def set_options(stages, opts, extra_opts, count) do
    Enum.map(stages, fn stage ->
      %{stage | opts: opts || %{}, extra_opts: extra_opts, count: count || 1}
    end)
  end

  defmacro __using__(opts) do
    quote do
      import ALF.DSL

      Module.register_attribute(__MODULE__, :stages, accumulate: false)

      @before_compile ALF.DSL

      def __pipeline__, do: true
      def __options__, do: unquote(opts)

      def done!(datum) do
        raise ALF.DoneStatement, datum
      end
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      def alf_components, do: @components
    end
  end

  def build_stage(atom, name, opts, extra_opts, count, current_module) do
    name = if name, do: name, else: atom

    if function_exported?(atom, :__info__, 1) do
      %Stage{
        pipe_module: current_module,
        pipeline_module: current_module,
        name: name,
        module: atom,
        function: :call,
        opts: opts || %{},
        extra_opts: extra_opts || false,
        count: count || 1
      }
    else
      %Stage{
        pipe_module: current_module,
        pipeline_module: current_module,
        name: name,
        module: current_module,
        function: atom,
        opts: opts || %{},
        extra_opts: extra_opts || false,
        count: count || 1
      }
    end
  end
end
