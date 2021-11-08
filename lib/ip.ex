defmodule ALF.IP do
  @moduledoc "Defines internal pipeline struct"

  defstruct [
    init_datum: nil,
    stream_ref: nil,
    ref: nil,
    datum: nil,
    error: nil,
    history: [],
    manager_name: nil
  ]

end
