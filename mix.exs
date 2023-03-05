defmodule Alf.MixProject do
  use Mix.Project

  def project do
    [
      app: :alf,
      version: "0.7.1",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      source_url: "https://github.com/antonmi/alf"
    ]
  end

  def application do
    [
      mod: {ALF.Application, []}
    ]
  end

  defp deps do
    [
      {:gen_stage, "~> 1.1"},
      {:telemetry, "~> 1.1"},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp description do
    "Flow-based Application Layer Framework"
  end

  defp package do
    [
      files: ~w(lib mix.exs README.md),
      maintainers: ["Anton Mishchuk"],
      licenses: ["MIT"],
      links: %{"github" => "https://github.com/antonmi/alf"}
    ]
  end
end
