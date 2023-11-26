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
    Composer,
    Tbd
  }

  alias ALF.DSLError

  defmacro stage(atom, options \\ [opts: []]) do
    count = options[:count] || 1
    opts = options[:opts]

    quote do
      Stage.validate_options(unquote(atom), unquote(options))
      Basic.build_component(Stage, unquote(atom), unquote(count), unquote(opts), __MODULE__)
    end
  end

  defmacro switch(atom, options \\ [opts: []]) do
    opts = options[:opts] || []
    count = options[:count] || 1

    quote do
      Switch.validate_options(unquote(atom), unquote(options))
      branches = build_branches(unquote(options)[:branches])

      switch =
        Basic.build_component(Switch, unquote(atom), unquote(count), unquote(opts), __MODULE__)

      %{switch | branches: branches}
    end
  end

  defmacro clone(name, options) do
    count = options[:count] || 1
    stages = options[:to]

    quote do
      Clone.validate_options(unquote(name), unquote(options))
      clone = Basic.build_component(Clone, unquote(name), unquote(count), %{}, __MODULE__)
      %{clone | to: unquote(stages)}
    end
  end

  defmacro goto(name, options \\ [opts: []]) do
    to = options[:to]
    opts = options[:opts] || []
    count = options[:count] || 1

    quote do
      Goto.validate_options(unquote(name), unquote(options))
      goto = Basic.build_component(Goto, unquote(name), unquote(count), unquote(opts), __MODULE__)
      %{goto | to: unquote(to)}
    end
  end

  defmacro goto_point(name, options \\ [count: 1]) do
    count = options[:count] || 1

    quote do
      Basic.build_component(GotoPoint, unquote(name), unquote(count), %{}, __MODULE__)
    end
  end

  defmacro dead_end(name, options \\ [count: 1]) do
    count = options[:count] || 1

    quote do
      Basic.build_component(DeadEnd, unquote(name), unquote(count), %{}, __MODULE__)
    end
  end

  defmacro done(name, options \\ [opts: []]) do
    count = options[:count] || 1
    opts = options[:opts] || []

    quote do
      Done.validate_options(unquote(name), unquote(options))

      Basic.build_component(Done, unquote(name), unquote(count), unquote(opts), __MODULE__)
    end
  end

  defmacro composer(name, options \\ [opts: []]) do
    opts = options[:opts] || []
    memo = options[:memo]
    count = options[:count] || 1

    quote do
      Composer.validate_options(unquote(name), unquote(options))

      composer =
        Basic.build_component(Composer, unquote(name), unquote(count), unquote(opts), __MODULE__)

      %{composer | memo: unquote(memo)}
    end
  end

  defmacro from(module, options \\ [opts: []]) do
    count = options[:count] || 1
    opts = options[:opts] || []

    quote do
      validate_stages_from_options(unquote(options))

      set_options(unquote(module).alf_components, unquote(opts), unquote(count))
    end
  end

  defmacro plug_with(module, options \\ [count: 1], do: block) do
    count = options[:count] || 1

    quote do
      validate_plug_with_options(unquote(options))
      plug = Basic.build_component(Plug, unquote(module), unquote(count), %{}, __MODULE__)

      unplug = Basic.build_component(Unplug, unquote(module), unquote(count), %{}, __MODULE__)

      [plug] ++ unquote(block) ++ [unplug]
    end
  end

  defmacro tbd() do
    quote do
      Basic.build_component(Tbd, unquote(:tbd), 1, %{}, __MODULE__)
    end
  end

  defmacro tbd(name, opts \\ []) do
    count = opts[:count] || 1

    quote do
      Tbd.validate_name(unquote(name))
      tbd = Basic.build_component(Tbd, unquote(name), unquote(count), %{}, __MODULE__)
      %{tbd | count: unquote(count)}
    end
  end

  def validate_stages_from_options(options) do
    dsl_options = [:count, :opts]
    wrong_options = Keyword.keys(options) -- dsl_options

    if Enum.any?(wrong_options) do
      raise DSLError,
            "Wrong options are given for the 'from' macro: #{inspect(wrong_options)}. " <>
              "Available options are #{inspect(dsl_options)}"
    end
  end

  def validate_plug_with_options(options) do
    dsl_options = [:module, :name, :opts, :count]
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

  def set_options(components, opts, count) do
    Enum.map(components, &%{&1 | opts: merge_opts(&1.opts, opts), count: count})
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
