defmodule ALF.DoneStatement do
  @moduledoc "Controls done! flow"

  defexception [:message, :datum, :ip]

  @impl true
  def exception(datum) do
    %ALF.DoneStatement{message: "done", datum: datum}
  end
end
