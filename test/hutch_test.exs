defmodule Hutch.HutchTest do
  @moduledoc false

  use ExUnit.Case, async: false
  import Mimic

  alias Hutch

  setup :set_mimic_global

  describe "do_create_queue/2" do
    setup do
      conn = %{}
      channel = %{}

      expect(AMQP.Connection, :open, fn ^conn -> {:ok, conn} end)
      expect(AMQP.Channel, :open, fn ^conn -> {:ok, channel} end)
      expect(AMQP.Connection, :close, fn ^conn -> :ok end)

      {:ok, conn: conn, channel: channel}
    end

    test "creates a queue without retry", %{conn: conn, channel: channel} do
      opts = [
        conn: conn,
        exchange: "ex",
        retry: false,
        durable: true,
        prefix: "prefix"
      ]

      expect(AMQP.Queue, :declare, fn ^channel, "prefix.queue_name", opts ->
        assert Keyword.get(opts, :durable) == true
        assert {"x-dead-letter-exchange", :longstr, ""} in opts[:arguments]
        {:ok, :queue}
      end)

      expect(AMQP.Queue, :declare, fn ^channel, "prefix.queue_name.rejected", opts ->
        assert Keyword.get(opts, :durable) == true

        assert {"x-message-ttl", :signedint, _} =
                 List.keyfind(opts[:arguments], "x-message-ttl", 0)

        {:ok, :rejected_queue}
      end)

      expect(AMQP.Exchange, :declare, fn ^channel, "ex", :topic -> :ok end)

      expect(AMQP.Queue, :bind, fn ^channel,
                                   "prefix.queue_name",
                                   "ex",
                                   routing_key: "queue_name" ->
        :ok
      end)

      result = Hutch.do_create_queue(channel, "queue_name", opts)
      assert result == :ok
    end

    test "creates retry queues when retry option enabled", %{conn: conn, channel: channel} do
      opts = [
        conn: conn,
        exchange: "ex",
        retry: true,
        durable: true,
        prefix: "prefix"
      ]

      expect(AMQP.Queue, :declare, fn ^channel, "prefix.queue_name", opts ->
        assert {"x-queue-type", :longstr, "quorum"} in opts[:arguments]
        {:ok, :quorum_queue}
      end)

      expect(AMQP.Queue, :declare, fn ^channel, "prefix.queue_name.retry", opts ->
        assert {"x-message-ttl", :signedint, _} =
                 List.keyfind(opts[:arguments], "x-message-ttl", 0)

        {:ok, :retry_queue}
      end)

      expect(AMQP.Queue, :declare, fn ^channel, "prefix.queue_name.rejected", _opts ->
        {:ok, :rejected_queue}
      end)

      expect(AMQP.Exchange, :declare, fn ^channel, "ex", :topic -> :ok end)
      expect(AMQP.Exchange, :declare, fn ^channel, "ex", :topic -> :ok end)

      expect(AMQP.Queue, :bind, fn ^channel,
                                   "prefix.queue_name",
                                   "ex",
                                   routing_key: "queue_name" ->
        :ok
      end)

      expect(AMQP.Queue, :bind, fn ^channel,
                                   "prefix.queue_name.retry",
                                   "ex",
                                   routing_key: "queue_name.retry" ->
        :ok
      end)

      result = Hutch.do_create_queue(channel, "queue_name", opts)
      assert result == :ok
    end
  end
end
