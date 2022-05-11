defmodule ALF.Manager.ProcessingOptions do
  defstruct chunk_every: 10,
            return_ips: false

  def new(opts) when is_list(opts) do
    struct(__MODULE__, Enum.into(opts, %{}))
  end
end
