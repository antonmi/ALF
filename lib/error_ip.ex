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
          pipeline_module: atom(),
          debug: boolean(),
          history: list(),
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
            pipeline_module: nil,
            debug: false,
            history: [],
            plugs: %{}
end
