import Config

config :alf,
  telemetry_enabled: false,
  auto_scaler_interval: 1000

import_config "#{config_env()}.exs"
