defmodule ALF.Source do
  @callback open(term, list) :: map
  @callback stream(map) :: (term, term -> term)
  @callback close(map) :: :ok
end
