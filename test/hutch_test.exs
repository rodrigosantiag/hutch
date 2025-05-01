defmodule HutchTest do
  use ExUnit.Case, async: true
  import Mimic

  setup :verify_on_exit!

  test "creates queue with retry: false" do
    expect(AMQP.Connection, :open, fn "amqp://localhost" -> {:ok, :conn} end)
    expect(AMQP.Channel, :open, fn :conn -> {:ok, :chan} end)
    expect(AMQP.Connection, :close, fn :conn -> :ok end)

    expect(AMQP.Queue, :declare, 2, fn :chan, _queue, _opts -> {:ok, %{}} end)
    expect(AMQP.Exchange, :declare, 2, fn :chan, _exchange, :topic -> :ok end)

    expect(AMQP.Queue, :bind, 2, fn :chan, _queue, _exchange, opts ->
      assert Keyword.has_key?(opts, :routing_key)
      :ok
    end)

    opts = [
      rabbit_url: "amqp://localhost",
      exchange: "myapp",
      prefix: "dev",
      retry: false
    ]

    assert :ok = Hutch.create_queue("myqueue", opts)
  end
end
