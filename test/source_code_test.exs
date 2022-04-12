Code.compile_file("test/source_code_test_module.ex")

defmodule ALF.SourceCodeTest do
  use ExUnit.Case, async: false
  alias ALF.SourceCode

  alias Test.Foo
  alias Test.Bar.Baz

  describe "module_source/1" do
    test "success cases" do
      assert SourceCode.module_source(Foo) ==
               "defmodule(Foo) do\n  def(foo_fun(:a)) do\n    :a\n  end\n  def(foo_fun(:b)) do\n    :b\n  end\n  def(bar) do\n    :bar\n  end\nend"

      assert SourceCode.module_source(Baz) ==
               "defmodule(Bar.Baz) do\n  def(baz(a, b)) do\n    div(a, b)\n  end\nend"

      assert SourceCode.module_source(Test2) ==
               "defmodule(Test2) do\n  def(aaa) do\n    :aaa\n  end\nend"
    end

    test "not found" do
      assert is_nil(SourceCode.module_source(NotFound))
    end
  end

  describe "function_source/2" do
    test "success cases for Test.Mod1" do
      assert SourceCode.function_source(Foo, :foo_fun) ==
               "def(foo_fun(:a)) do\n  :a\nend\ndef(foo_fun(:b)) do\n  :b\nend"

      assert SourceCode.function_source(Foo, :bar) == "def(bar) do\n  :bar\nend"
      assert SourceCode.function_source(Baz, :baz) == "def(baz(a, b)) do\n  div(a, b)\nend"
    end

    test "not found" do
      assert is_nil(SourceCode.function_source(NotFound, :not_found))
      assert is_nil(SourceCode.function_source(Foo, :not_found))
    end
  end
end
