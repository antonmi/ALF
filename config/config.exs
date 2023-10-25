import Config

config :alf,
  telemetry_enabled: false,
  default_timeout: 10_000

import_config "#{config_env()}.exs"
