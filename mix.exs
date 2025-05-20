defmodule Hutch.MixProject do
  use Mix.Project

  def project do
    [
      app: :hutch,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: [
        main: "Hutch",
        source_url: "https://github.com/hammeraj/hutch",
        extras: ["README.md"]
      ],
      elixirc_paths: elixirc_paths(Mix.env())
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:amqp, "~> 1.3 or ~> 2.0 or ~> 3.0 or ~> 4.0"},
      {:broadway, "~> 1.0"},
      {:broadway_rabbitmq, "~> 0.6"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.38.1", only: :dev, runtime: false},
      {:jason, "~> 1.4"},
      {:mimic, "~> 1.0", only: :test}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
