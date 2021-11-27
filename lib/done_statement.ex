defmodule ALF.DoneStatement do
  @moduledoc "Controls done! flow"

  defexception [:message, :event, :ip]

  @impl true
  def exception(event) do
    %ALF.DoneStatement{message: "done", event: event}
  end
end
