defmodule Test do
  defmodule Foo do
    def foo_fun(:a) do
      :a
    end

    def foo_fun(:b) do
      :b
    end

    def bar do
      :bar
    end
  end

  defmodule Bar.Baz do
    def baz(a, b) do
      div(a, b)
    end
  end
end

defmodule Test2 do
  def aaa, do: :aaa
end
