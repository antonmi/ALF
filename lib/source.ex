defmodule ALF.Source do
  @callback open(term, list) :: map
  @callback call(map) :: {:ok, term} | {:error, term}
  @callback stream(map) :: (term, term -> term)
  @callback close(map) :: :ok
end
