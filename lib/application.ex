defmodule ALF.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: ALF.DynamicSupervisor},
      {ALF.Introspection, []},
      {ALF.TelemetryBroadcaster, []},
      {ALF.AutoScaler, []}
    ]

    opts = [name: ALF.Supervisor, strategy: :one_for_one]
    Supervisor.start_link(children, opts)
  end
end
