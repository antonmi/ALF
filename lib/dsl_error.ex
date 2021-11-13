defmodule ALF.DSLError do
  @moduledoc "Controls done! flow"

  defexception [:message]

  @impl true
  def exception(message) do
    %ALF.DSLError{message: message}
  end
end
