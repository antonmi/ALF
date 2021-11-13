defmodule ALF.DSL do
  alias ALF.Components.{
    Stage,
    Switch,
    Clone,
    DeadEnd,
    GotoPoint,
    Goto
  }

  alias ALF.DSLError

  defmacro stage(atom, options \\ [opts: [], count: 1, name: nil]) do
    count = options[:count]
    opts = options[:opts]
    name = options[:name]

    quote do
      Stage.validate_options(unquote(atom), unquote(options))

      build_stage(
        unquote(atom),
        unquote(name),
        unquote(opts),
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

  defmacro goto(name, options \\ [to: [], if: nil, opts: []]) do
    to = options[:to]
    iff = options[:if]
    opts = options[:opts]

    quote do
      Goto.validate_options(unquote(name), unquote(options))

      %Goto{
        name: unquote(name),
        to: unquote(to),
        if: unquote(iff),
        opts: unquote(opts),
        pipe_module: __MODULE__,
        pipeline_module: __MODULE__
      }
    end
  end

  defmacro stages_from(module, options \\ [opts: [], count: 1]) do
    count = options[:count]
    opts = options[:opts]

    quote do
      validate_stages_from_options(unquote(module), unquote(options))

      unquote(module).alf_components
      |> ALF.DSL.set_pipeline_module(__MODULE__)
      |> ALF.DSL.set_options(unquote(opts), unquote(count))
    end
  end

  def validate_stages_from_options(module, options) do
    dsl_options = [:count, :opts]
    wrong_options = Keyword.keys(options) -- dsl_options

    unless module_exist?(module) do
      raise DSLError, "There is no such module: #{inspect(module)}"
    end

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options are given for the stages_from macro: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(dsl_options)}"
    end
  end

  defmacro switch(name, options) do
    quote do
      Switch.validate_options(unquote(name), unquote(options))
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
      Clone.validate_options(unquote(name), unquote(options))
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

  def set_options(stages, opts, count) do
    Enum.map(stages, fn stage ->
      opts = merge_opts(stage.opts, opts)
      %{stage | opts: opts, count: count || 1}
    end)
  end

  defp merge_opts(opts, new_opts) do
    opts = if is_map(opts), do: Map.to_list(opts), else: opts
    new_opts = if is_map(new_opts), do: Map.to_list(new_opts), else: new_opts
    Keyword.merge(opts, new_opts)
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

  def build_stage(atom, name, opts, count, current_module) do
    name = if name, do: name, else: atom

    if module_exist?(atom) do
      %Stage{
        pipe_module: current_module,
        pipeline_module: current_module,
        name: name,
        module: atom,
        function: :call,
        opts: opts || %{},
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
        count: count || 1
      }
    end
  end

  def module_exist?(module), do: function_exported?(module, :__info__, 1)
end
