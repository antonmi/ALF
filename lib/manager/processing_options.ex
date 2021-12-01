defmodule ALF.Manager.ProcessingOptions do
  defstruct chunk_every: 10,
            return_ips: false

  def new(map) when is_map(map) do
    struct(__MODULE__, map)
  end
end
