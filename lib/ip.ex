defmodule ALF.IP do
  @moduledoc "Defines internal pipeline struct"

  defstruct init_datum: nil,
            stream_ref: nil,
            ref: nil,
            datum: nil,
            history: [],
            manager_name: nil,
            in_progress: false,
            decomposed: false,
            recomposed: false,
            plugs: %{}
end
