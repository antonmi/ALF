defmodule ALF.ErrorIP do
  @moduledoc "Defines internal pipeline struct"

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
            new_stream_ref: nil,
            in_progress: false,
            decomposed: false,
            recomposed: false,
            plugs: %{}
end
