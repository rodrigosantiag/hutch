defmodule Hutch do
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @rabbit_url Keyword.fetch!(opts, :rabbit_url)
      @prefix Keyword.fetch!(opts, :prefix)
      # 14 days
      @hutch_default_dlq_ttl :timer.hours(24) * 14
      @hutch_default_attempts 2
      @default_dlq_ttl Keyword.get(opts, :dlq_ttl, @hutch_default_dlq_ttl)
      @default_retry_attempts Keyword.get(opts, :retry_attempts, @hutch_default_attempts)

      def rabbit_url, do: @rabbit_url
      def prefix, do: @prefix
      def default_dlq_ttl, do: @default_dlq_ttl
      def default_retry_attempts, do: @default_retry_attempts
    end
  end

  @default_retry_interval :timer.minutes(2)

  @spec create_queue(String.t(), Keyword.t()) :: :ok | {:error, any()}
  def create_queue(queue_name, opts) do
    connection_string = Keyword.fetch!(opts, :rabbit_url)

    with_channel(connection_string, fn channel ->
      create_queue(channel, queue_name, opts)
    end)
  end

  @spec create_queue(AMQP.Channel.t(), String.t(), Keyword.t()) :: :ok | {:error, any()}
  def create_queue(channel, queue_name, opts) do
    config = build_config(queue_name, opts)

    bindings = [
      binding(config.exchange, config.final_queue_name, config.queue_name),
      binding(
        config.dead_letter_exchange,
        config.dead_letter_queue,
        config.dead_letter_routing_key
      )
    ]

    declare_base_queue(
      channel,
      config.dead_letter_exchange,
      config.final_queue_name,
      config.retry_attempts,
      config.retry_ttl,
      config.dead_letter_routing_key,
      config.durable
    )

    declare_expire_queue(channel, config.dead_letter_queue, config.ttl, config.durable)
    bind_queues(channel, bindings)
  end

  defp binding(exchange, queue, routing_key),
    do: [exchange: exchange, queue_name: queue, routing_key: routing_key]

  defp build_config(queue_name, opts) do
    exchange = Keyword.fetch!(opts, :exchange)
    durable = Keyword.get(opts, :durable, true)
    retry_attempts = Keyword.get(opts, :retry_attempts)
    retry_ttl = Keyword.get(opts, :retry_interval, @default_retry_interval)
    ttl = Keyword.get(opts, :ttl)
    prefix = Keyword.get(opts, :prefix) <> "."

    final_queue_name = prefix <> queue_name
    dead_letter_queue = final_queue_name <> ".dlq"
    dead_letter_exchange = exchange <> ".dlq"
    dead_letter_routing_key = queue_name <> ".dlq"
    retry_queue = final_queue_name <> ".retry"
    retry_exchange = exchange <> ".retry"
    retry_routing_key = queue_name <> ".retry"

    %{
      exchange: exchange,
      durable: durable,
      retry_attempts: retry_attempts,
      retry_ttl: retry_ttl,
      ttl: ttl,
      queue_name: queue_name,
      prefix: prefix,
      final_queue_name: final_queue_name,
      dead_letter_queue: dead_letter_queue,
      dead_letter_exchange: dead_letter_exchange,
      dead_letter_routing_key: dead_letter_routing_key,
      retry_queue: retry_queue,
      retry_exchange: retry_exchange,
      retry_routing_key: retry_routing_key
    }
  end

  defp bind_queues(channel, queues) do
    Enum.each(queues, fn queue ->
      AMQP.Exchange.declare(channel, queue[:exchange], :topic)

      AMQP.Queue.bind(
        channel,
        queue[:queue_name],
        queue[:exchange],
        routing_key: queue[:routing_key]
      )
    end)
  end

  defp with_channel(connection_string, fun) do
    {:ok, connection} = AMQP.Connection.open(connection_string)
    {:ok, channel} = AMQP.Channel.open(connection)
    result = fun.(channel)
    AMQP.Connection.close(connection)
    result
  end

  defp declare_expire_queue(channel, queue_name, expiry, durable) do
    AMQP.Queue.declare(channel, queue_name,
      durable: durable,
      arguments: [
        {"x-message-ttl", :signedint, expiry}
      ]
    )
  end

  defp declare_base_queue(
         channel,
         exchange,
         queue_name,
         number_attempts,
         retry_ttl,
         routing_key,
         durable
       ) do
    AMQP.Queue.declare(channel, queue_name,
      durable: durable,
      arguments: [
        {"x-queue-type", :longstr, "quorum"},
        {"x-delivery-limit", :signedint, number_attempts},
        {"x-message-ttl", :signedint, retry_ttl},
        {"x-dead-letter-strategy", :longstr, "at-least-once"},
        {"x-overflow", :longstr, "reject-publish"},
        {"x-dead-letter-exchange", :longstr, exchange},
        {"x-dead-letter-routing-key", :longstr, routing_key}
      ]
    )
  end
end
