defmodule ALF.SourceCode do
  @moduledoc "Extracts source code"

  @line_length 60

  @spec module_doc(atom()) :: String.t() | nil
  def module_doc(module) when is_atom(module) do
    case Code.fetch_docs(module) do
      {:docs_v1, _, _, _, %{} = doc, _, _} ->
        doc["en"]

      {:docs_v1, _, _, _, _, _, _} ->
        nil

      {:error, :module_not_found} ->
        nil
    end
  end

  @spec function_doc(atom(), atom()) :: String.t() | nil
  def function_doc(module, function) when is_atom(module) do
    case Code.fetch_docs(module) do
      {:docs_v1, _, _, _, _, _, docs} ->
        case Enum.find(docs, fn
               {{kind, function_name, arity}, _, _, _, _} ->
                 kind == :function and
                   function_name == function and
                   arity == 2
             end) do
          {_, _, _, %{} = doc, _} ->
            doc["en"]

          nil ->
            nil
        end

      {:error, :module_not_found} ->
        nil
    end
  end

  @spec module_source(atom()) :: String.t() | nil
  def module_source(module) when is_atom(module) do
    case module_ast(module) do
      nil ->
        nil

      ast ->
        format_ast(ast)
    end
  end

  @spec function_source(atom(), atom() | (... -> any())) :: String.t() | nil
  def function_source(module, function) when is_atom(module) and is_atom(function) do
    case function_asts(module, function) do
      [] ->
        nil

      asts ->
        asts
        |> Enum.map(&format_ast/1)
        |> Enum.join("\n\n")
    end
  end

  def function_source(module, function) when is_atom(module) and is_function(function) do
    inspect(function)
  end

  defp format_ast(ast) do
    ast
    |> Macro.to_string()
    |> Code.format_string!(line_length: @line_length)
    |> Enum.join()
  end

  defp function_asts(module, function) do
    case module_ast(module) do
      nil ->
        []

      module_ast ->
        case module_ast do
          {:defmodule, _, [_aliases, [do: {:__block__, _, fun_asts}]]} ->
            find_functions(fun_asts, function)

          {:defmodule, _, [_aliases, [do: fun_ast]]} ->
            find_functions([fun_ast], function)
        end
    end
  end

  defp find_functions(fun_asts, function) do
    fun_asts
    |> Enum.filter(fn fun_ast ->
      case fun_ast do
        {:def, _, [{^function, _, _args}, _do_block]} ->
          fun_ast

        _ ->
          false
      end
    end)
  end

  defp module_ast(module) do
    if module_exist?(module) do
      ast = read_ast_from_source_file(module)
      result = traverse_modules(ast, %{}, [])
      module_aliases = split_module_to_aliases(module)
      Map.get(result, module_aliases, nil)
    end
  end

  defp read_ast_from_source_file(module) do
    module.module_info(:compile)[:source]
    |> File.read()
    |> case do
      {:ok, content} ->
        Code.string_to_quoted!(content)

      {:error, :enoent} ->
        ""
    end
  end

  defp split_module_to_aliases(module) do
    module
    |> Module.split()
    |> Enum.map(&String.to_atom(&1))
  end

  defp traverse_modules({:__block__, _, tree}, modules_acc, aliases_before) do
    tree
    |> Enum.reduce(modules_acc, fn subtree, acc ->
      Map.merge(acc, traverse_modules(subtree, modules_acc, aliases_before))
    end)
  end

  defp traverse_modules(
         {:defmodule, _,
          [
            {:__aliases__, _, aliases},
            [do: tree]
          ]} = subtree,
         modules_acc,
         aliases_before
       ) do
    modules_acc
    |> Map.put(aliases_before ++ aliases, subtree)
    |> Map.merge(traverse_modules(tree, %{}, aliases_before ++ aliases))
  end

  defp traverse_modules(_tree, _modules_acc, _aliases), do: %{}

  defp module_exist?(module), do: function_exported?(module, :__info__, 1)
end
