defmodule ALF.DSLError do
  @moduledoc "ALF.DSLError"

  defexception [:message]

  @impl true
  def exception(message) do
    %ALF.DSLError{message: message}
  end
end
