defmodule TestHelpers.MockQueueManager do
  @moduledoc false

  def rabbit_url, do: "amqp://localhost"
  def prefix, do: "test"
  def create_queue(_, _opts), do: :ok
end
