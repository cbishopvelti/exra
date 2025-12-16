import Config

config :logger, :console,
  # Option A: whitelist specific keys (Recommended)
  metadata: [:term, :name]

  # Option B: show ALL metadata (Good for debugging, noisy in prod)
  # metadata: :all

  # Optional: Define how the log string looks ($metadata inserts it)
  # format: "$time $metadata[$level] $message\n"
