defmodule ALF.Sink do
  @callback open(term, list) :: map
  @callback stream((term, term -> term), map) :: (term, term -> term)
  @callback close(map) :: :ok
end
