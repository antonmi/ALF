defmodule ALF.IP do
  @moduledoc "Defines internal pipeline struct"

  @type t :: %__MODULE__{
          type: :ip,
          event: any(),
          init_event: any(),
          destination: pid(),
          ref: reference(),
          stream_ref: reference() | nil,
          pipeline_module: atom(),
          debug: boolean(),
          history: list(),
          decomposed: boolean(),
          recomposed: boolean(),
          composed: boolean(),
          plugs: map(),
          sync_path: nil | list()
        }

  defstruct type: :ip,
            event: nil,
            init_event: nil,
            destination: nil,
            ref: nil,
            stream_ref: nil,
            pipeline_module: nil,
            debug: false,
            history: [],
            decomposed: false,
            recomposed: false,
            composed: false,
            plugs: %{},
            sync_path: nil
end
