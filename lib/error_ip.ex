defmodule ALF.ErrorIP do
  @moduledoc "Defines internal pipeline struct"

  @type t :: %__MODULE__{}
  # TODO revise the list
  defstruct type: :error_ip,
            ip: nil,
            error: nil,
            destination: nil,
            stacktrace: nil,
            component: nil,
            manager_name: nil,
            ref: nil,
            stream_ref: nil,
            decomposed: false,
            recomposed: false,
            plugs: %{}
end
