Code.ensure_loaded(ALF.Test.Foo)
Code.ensure_loaded(ALF.Test.Bar.Baz)
Code.ensure_loaded(ALF.Test2)

defmodule ALF.SourceCodeTest do
  use ExUnit.Case, async: false
  alias ALF.SourceCode

  alias ALF.Test.Foo
  alias ALF.Test.Bar.Baz
  alias ALF.Test2

  describe "module_doc/1" do
    test "success case" do
      assert SourceCode.module_doc(Foo) == "Docs for Foo"
      assert is_nil(SourceCode.module_doc(Baz))
      assert SourceCode.module_doc(Test2) == "Docs for Test2"
    end

    test "not found" do
      assert is_nil(SourceCode.module_doc(NotFound))
    end
  end

  describe "module_doc/2" do
    test "success case" do
      assert SourceCode.function_doc(Foo, :foo_fun) == "it's foo_fun function"
      assert SourceCode.function_doc(Foo, :bar) == "it's bar function"
      assert is_nil(SourceCode.function_doc(Baz, :baz))
      assert SourceCode.function_doc(Test2, :aaa) == "it's aaa function"
    end

    test "not found" do
      assert is_nil(SourceCode.function_doc(NotFound, :not_found))
      assert is_nil(SourceCode.function_doc(Foo, :not_foo_fun))
    end
  end

  describe "module_source/1" do
    test "success cases" do
      assert String.starts_with?(SourceCode.module_source(Foo), "defmodule(Foo) do")
      assert String.starts_with?(SourceCode.module_source(Baz), "defmodule(Bar.Baz) do")
      assert String.starts_with?(SourceCode.module_source(Test2), "defmodule(ALF.Test2) do")
    end

    test "not found" do
      assert is_nil(SourceCode.module_source(NotFound))
    end

    test "no source" do
      assert is_nil(SourceCode.module_source(Module))
    end
  end

  describe "function_source/2" do
    test "success cases for Test.Mod1" do
      assert SourceCode.function_source(Foo, :foo_fun) ==
               "def(foo_fun(:a, :opts)) do\n  :a\nend\ndef(foo_fun(:b, :opts)) do\n  :b\nend"

      assert SourceCode.function_source(Foo, :bar) == "def(bar(:bar, :opts)) do\n  :bar\nend"
      assert SourceCode.function_source(Baz, :baz) == "def(baz(a, b)) do\n  div(a, b)\nend"
    end

    test "not found" do
      assert is_nil(SourceCode.function_source(NotFound, :not_found))
      assert is_nil(SourceCode.function_source(Foo, :not_found))
    end

    test "when second argument is a function" do
      source = SourceCode.function_source(Foo, fn -> 1 + 1 end)
      assert String.starts_with?(source, "#Function<")
    end
  end
end
