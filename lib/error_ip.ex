defmodule ALF.ErrorIP do
  @moduledoc "Defines internal pipeline struct"

  @type t :: %__MODULE__{
          type: :error_ip,
          ip: ALF.IP.t(),
          error: any(),
          destination: pid(),
          ref: reference(),
          stream_ref: reference() | nil,
          stacktrace: list(),
          component: map(),
          manager_name: atom(),
          decomposed: boolean(),
          recomposed: boolean(),
          plugs: map()
        }

  defstruct type: :error_ip,
            ip: nil,
            error: nil,
            destination: nil,
            ref: nil,
            stream_ref: nil,
            stacktrace: nil,
            component: nil,
            manager_name: nil,
            decomposed: false,
            recomposed: false,
            plugs: %{}
end
