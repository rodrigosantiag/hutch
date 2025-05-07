defmodule Hutch do
  @moduledoc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      require Logger
      # TODO: Queue creation OK. Now, implement retry logic.
      # Required options
      @rabbit_url Keyword.fetch!(opts, :rabbit_url)
      @prefix Keyword.fetch!(opts, :prefix)

      # Defaults
      # Default DLQ TTL 14 days
      @hutch_default_dlq_ttl :timer.hours(24) * 14
      @hutch_default_attempts 10
      @hutch_default_retry_interval :timer.minutes(2)

      # Optional options
      @dlq_ttl Keyword.get(opts, :dlq_ttl, @hutch_default_dlq_ttl)
      @retry_attempts Keyword.get(opts, :retry_attempts, @hutch_default_attempts)
      @retry_interval Keyword.get(opts, :retry_interval, @hutch_default_retry_interval)
      @retry Keyword.get(opts, :retry, false)

      def rabbit_url, do: @rabbit_url
      def prefix, do: @prefix
      def dlq_ttl, do: @dlq_ttl
      def retry_attempts, do: @retry_attempts
      def retry_interval, do: @retry_interval
      def retry, do: @retry

      @spec create_queue(String.t(), Keyword.t()) :: :ok | {:error, any()}
      def create_queue(queue_name, opts) do
        with_channel(fn channel ->
          create_queue(channel, queue_name, opts)
        end)
      end

      @spec create_queue(AMQP.Channel.t(), String.t(), Keyword.t()) :: :ok | {:error, any()}
      def create_queue(channel, queue_name, opts) when @retry == true do
        config = build_config(queue_name, opts)

        bindings = [
          binding(config.exchange, config.final_queue_name, config.queue_name),
          binding(config.exchange, config.retry_queue, config.retry_routing_key)
        ]

        declare_quorum_base_queue(
          channel,
          config.final_queue_name,
          config.retry_queue,
          config.durable
        )

        declare_retry_queue(
          channel,
          config.retry_queue,
          config.final_queue_name,
          config.retry_interval,
          config.durable
        )

        declare_rejected_queue(channel, config.rejected_queue, config.dlq_ttl, config.durable)
        bind_queues(channel, bindings)
      end

      def create_queue(channel, queue_name, opts) do
        config = build_config(queue_name, opts)

        bindings = [
          binding(config.exchange, config.final_queue_name, config.queue_name)
        ]

        declare_base_queue(
          channel,
          config.final_queue_name,
          config.rejected_queue,
          config.durable
        )

        declare_rejected_queue(channel, config.rejected_queue, config.dlq_ttl, config.durable)
        bind_queues(channel, bindings)
      end

      defp binding(exchange, queue, routing_key),
        do: [exchange: exchange, queue_name: queue, routing_key: routing_key]

      defp build_config(queue_name, opts) do
        Logger.info("Queue name: #{queue_name}")
        Logger.info("Options: #{inspect(opts)}")
        exchange = Keyword.fetch!(opts, :exchange)
        durable = Keyword.get(opts, :durable, true)

        final_queue_name = prefix() <> "." <> queue_name
        rejected_queue = final_queue_name <> ".rejected"
        rejected_exchange = exchange <> ".rejected"
        rejected_routing_key = queue_name <> ".rejected"
        retry_queue = final_queue_name <> ".retry"
        retry_exchange = exchange <> ".retry"
        retry_routing_key = queue_name <> ".retry"

        %{
          exchange: exchange,
          durable: durable,
          retry_attempts: retry_attempts(),
          retry_interval: retry_interval(),
          dlq_ttl: dlq_ttl(),
          queue_name: queue_name,
          prefix: prefix(),
          final_queue_name: final_queue_name,
          rejected_queue: rejected_queue,
          rejected_exchange: rejected_exchange,
          rejected_routing_key: rejected_routing_key,
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

      defp with_channel(fun) do
        Logger.info("Function: #{inspect(fun)}")
        Logger.info("RabbitMQ connection: #{rabbit_url()}")
        {:ok, connection} = AMQP.Connection.open(rabbit_url())
        {:ok, channel} = AMQP.Channel.open(connection)
        result = fun.(channel)
        AMQP.Connection.close(connection)
        result
      end

      defp declare_rejected_queue(channel, queue_name, message_ttl, durable) do
        AMQP.Queue.declare(channel, queue_name,
          durable: durable,
          arguments: [
            {"x-message-ttl", :signedint, message_ttl}
          ]
        )
      end

      defp declare_base_queue(
             channel,
             queue_name,
             dead_letter_routing_key,
             durable
           ) do
        AMQP.Queue.declare(channel, queue_name,
          durable: durable,
          arguments: [
            {"x-dead-letter-exchange", :longstr, ""},
            {"x-dead-letter-routing-key", :longstr, dead_letter_routing_key}
          ]
        )
      end

      defp declare_quorum_base_queue(
             channel,
             queue_name,
             routing_key,
             durable
           ) do
        AMQP.Queue.declare(channel, queue_name,
          durable: durable,
          arguments: [
            {"x-queue-type", :longstr, "quorum"},
            {"x-dead-letter-strategy", :longstr, "at-least-once"},
            {"x-dead-letter-exchange", :longstr, ""},
            {"x-dead-letter-routing-key", :longstr, routing_key}
          ]
        )
      end

      defp declare_retry_queue(
             channel,
             queue_name,
             dead_letter_routing_key,
             message_ttl,
             durable
           ) do
        AMQP.Queue.declare(channel, queue_name,
          durable: durable,
          arguments: [
            {"x-message-ttl", :signedint, message_ttl},
            {"x-dead-letter-exchange", :longstr, ""},
            {"x-dead-letter-routing-key", :longstr, dead_letter_routing_key}
          ]
        )
      end
    end
  end
end
