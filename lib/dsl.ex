defmodule ALF.DSL do
  alias ALF.Components.{
    Basic,
    Stage,
    Switch,
    Clone,
    DeadEnd,
    GotoPoint,
    Goto,
    Done,
    Plug,
    Unplug,
    Decomposer,
    Recomposer,
    Composer,
    Tbd
  }

  alias ALF.DSLError

  defmacro stage(atom, options \\ [opts: [], count: 1, name: nil]) do
    count = options[:count]
    opts = options[:opts]
    name = options[:name]

    quote do
      Stage.validate_options(unquote(atom), unquote(options))

      stage =
        Basic.build_component(
          Stage,
          unquote(atom),
          unquote(name),
          unquote(opts),
          __MODULE__
        )

      %{stage | count: unquote(count) || 1}
    end
  end

  defmacro switch(atom, options \\ [opts: [], name: nil]) do
    opts = options[:opts]
    name = options[:name]

    quote do
      Switch.validate_options(unquote(atom), unquote(options))
      branches = build_branches(unquote(options)[:branches])

      switch =
        Basic.build_component(
          Switch,
          unquote(atom),
          unquote(name),
          unquote(opts),
          __MODULE__
        )

      %{switch | branches: branches}
    end
  end

  defmacro clone(name, options) do
    quote do
      Clone.validate_options(unquote(name), unquote(options))
      stages = unquote(options)[:to]

      %Clone{name: unquote(name), to: stages}
    end
  end

  defmacro goto(atom, options \\ [name: nil, opts: []]) do
    to = options[:to]
    opts = options[:opts]
    name = options[:name]

    quote do
      Goto.validate_options(unquote(atom), unquote(options))

      goto =
        Basic.build_component(
          Goto,
          unquote(atom),
          unquote(name),
          unquote(opts),
          __MODULE__
        )

      %{goto | to: unquote(to)}
    end
  end

  defmacro goto_point(name) do
    quote do
      %GotoPoint{name: unquote(name)}
    end
  end

  defmacro dead_end(name) do
    quote do
      %DeadEnd{name: unquote(name)}
    end
  end

  defmacro done(atom, options \\ [name: nil, opts: []]) do
    opts = options[:opts]
    name = options[:name]

    quote do
      Done.validate_options(unquote(atom), unquote(options))

      Basic.build_component(
        Done,
        unquote(atom),
        unquote(name),
        unquote(opts),
        __MODULE__
      )
    end
  end

  defmacro decomposer(atom, options \\ [opts: [], name: nil]) do
    opts = options[:opts]
    name = options[:name]

    quote do
      Decomposer.validate_options(unquote(atom), unquote(options))

      Basic.build_component(
        Decomposer,
        unquote(atom),
        unquote(name),
        unquote(opts),
        __MODULE__
      )
    end
  end

  defmacro recomposer(atom, options \\ [opts: [], name: nil]) do
    opts = options[:opts]
    name = options[:name]

    quote do
      Recomposer.validate_options(unquote(atom), unquote(options))

      Basic.build_component(
        Recomposer,
        unquote(atom),
        unquote(name),
        unquote(opts),
        __MODULE__
      )
    end
  end

  defmacro composer(atom, options \\ [opts: []]) do
    opts = options[:opts]
    name = options[:name]
    acc = options[:acc]

    quote do
      Composer.validate_options(unquote(atom), unquote(options))

      composer =
        Basic.build_component(
          Composer,
          unquote(atom),
          unquote(name),
          unquote(opts),
          __MODULE__
        )

      %{composer | acc: unquote(acc)}
    end
  end

  defmacro stages_from(module, options \\ [opts: [], count: 1]) do
    count = options[:count]
    opts = options[:opts]

    quote do
      validate_stages_from_options(unquote(options))

      set_options(unquote(module).alf_components, unquote(opts), unquote(count))
    end
  end

  defmacro plug_with(module, options \\ [opts: [], name: nil], do: block) do
    name = options[:name]

    quote do
      validate_plug_with_options(unquote(options))
      name = if unquote(name), do: unquote(name), else: unquote(module)
      plug = %Plug{name: name, module: unquote(module)}
      unplug = %Unplug{name: name, module: unquote(module)}

      [plug] ++ unquote(block) ++ [unplug]
    end
  end

  defmacro tbd(atom \\ :tbd) do
    quote do
      Tbd.validate_name(unquote(atom))

      Basic.build_component(
        Tbd,
        unquote(atom),
        unquote(atom),
        %{},
        __MODULE__
      )
    end
  end

  def validate_stages_from_options(options) do
    dsl_options = [:count, :opts]
    wrong_options = Keyword.keys(options) -- dsl_options

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options are given for the stages_from macro: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(dsl_options)}"
    end
  end

  def validate_plug_with_options(options) do
    dsl_options = [:module, :name, :opts]
    wrong_options = Keyword.keys(options) -- dsl_options

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options are given for the plug_with macro: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(dsl_options)}"
    end
  end

  def build_branches(branches) do
    branches
    |> Enum.reduce(%{}, fn {key, stages}, final_specs ->
      Map.put(final_specs, key, stages)
    end)
  end

  def set_options(stages, opts, count) do
    Enum.map(stages, fn stage ->
      case stage do
        %Stage{} ->
          %{stage | opts: merge_opts(stage.opts, opts), count: count || 1}

        stage ->
          %{stage | opts: merge_opts(stage.opts, opts)}
      end
    end)
  end

  defp merge_opts(opts, new_opts) do
    opts = if is_map(opts), do: Map.to_list(opts), else: opts
    new_opts = if is_map(new_opts), do: Map.to_list(new_opts), else: new_opts
    Keyword.merge(opts, new_opts)
  end

  defmacro __using__(_opts) do
    quote do
      import ALF.DSL

      @before_compile ALF.DSL

      @spec start() :: :ok
      def start() do
        ALF.Manager.start(__MODULE__, [])
      end

      @spec start(list) :: :ok
      def start(opts) when is_list(opts) do
        ALF.Manager.start(__MODULE__, opts)
      end

      @spec started?() :: true | false
      def started?() do
        ALF.Manager.started?(__MODULE__)
      end

      @spec stop() :: :ok | {:exit, {atom, any}}
      def stop do
        ALF.Manager.stop(__MODULE__)
      end

      @spec call(any, Keyword.t()) :: any | [any] | nil
      def call(event, opts \\ [debug: false]) do
        ALF.Manager.call(event, __MODULE__, opts)
      end

      @spec call(any, Keyword.t()) :: reference
      def cast(event, opts \\ [debug: false, send_result: false]) do
        ALF.Manager.cast(event, __MODULE__, opts)
      end

      @spec stream(Enumerable.t(), Keyword.t()) :: Enumerable.t()
      def stream(stream, opts \\ [debug: false]) do
        ALF.Manager.stream(stream, __MODULE__, opts)
      end

      @spec components() :: list(map())
      def components() do
        ALF.Manager.components(__MODULE__)
      end
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      def alf_components, do: List.flatten(@components)
    end
  end
end
