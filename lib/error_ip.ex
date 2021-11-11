defmodule ALF.ErrorIP do
  @moduledoc "Defines internal pipeline struct"

  defstruct ip: nil,
            error: nil,
            component: nil,
            manager_name: nil,
            ref: nil,
            stream_ref: nil
end
