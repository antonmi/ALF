defmodule ALF.Test do
  defmodule Foo do
    @moduledoc "Docs for Foo"

    @doc "it's foo_fun function"
    def foo_fun(:a, :opts) do
      :a
    end

    def foo_fun(:b, :opts) do
      :b
    end

    @doc "it's bar function"
    def bar(:bar, :opts) do
      :bar
    end
  end

  defmodule Bar.Baz do
    def baz(a, b) do
      div(a, b)
    end
  end
end

defmodule ALF.Test2 do
  @moduledoc "Docs for Test2"

  @doc "it's aaa function"
  def aaa(:aaa, :opts), do: :aaa
end
