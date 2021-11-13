defmodule ALF.DSLTest do
  use ExUnit.Case, async: true

  alias ALF.{Builder, DSLError}
  alias ALF.Components.{Stage, Switch, Clone, DeadEnd, GotoPoint, Goto}

  setup do
    sup_pid = Process.whereis(ALF.DynamicSupervisor)
    %{sup_pid: sup_pid}
  end

  describe "Stage validations" do
    test "wrong options", %{sup_pid: sup_pid} do
      assert_raise DSLError,
                   "Wrong options for the Elixir.ModuleA stage: [:bla]",
                   fn ->
                     defmodule StageWithWrongOptions do
                       use ALF.DSL

                       @components [stage(ModuleA, bla: :bla)]
                     end
                   end
    end

    test "string instead of atom", %{sup_pid: sup_pid} do
      assert_raise DSLError,
                   "Stage must be an atom: \"ModuleA\"",
                   fn ->
                     defmodule StageWithWrongOptions do
                       use ALF.DSL

                       @components [stage("ModuleA")]
                     end
                   end
    end
  end

  describe "Switch" do
    test "build StageWithWrongOptions", %{sup_pid: sup_pid} do
      assert_raise DSLError,
                   "Wrong options for the Elixir.ModuleA stage: [:bla]",
                   fn ->
                     defmodule StageWithWrongOptions do
                       use ALF.DSL

                       @components [stage(ModuleA, bla: :bla)]
                     end
                   end
    end
  end
end
