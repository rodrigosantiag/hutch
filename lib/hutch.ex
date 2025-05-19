defmodule Hutch do
  @moduledoc """
  Central module for Hutch configuration and RabbitMQ queue management.

  This module is intended to be `use`d in your application's Hutch setup module
  (e.g., `MyApp.Hutch`). It provides functions to access shared configurations
  like the RabbitMQ URL and a global prefix for queues/exchanges.

  More importantly, it provides the `create_queue/2` function for declaring
  RabbitMQ queues with retry and dead-letter queue (DLQ) support, simplifying
  the setup of robust messaging infrastructure.

  ## Example Usage

  ```elixir
  defmodule MyApp.Hutch do
    use Hutch,
      rabbit_url: "amqp://guest:guest@localhost",
      prefix: "my_app"

    # Hutch.create_queue/2 can be used to create queues with retry and DLQ support.
    # For example, to create a queue with retry logic:
    # def setup_queues do
    #   create_queue("my_queue", exchange: "my_exchange", retry: true)
    # end
  end
  ```
  When you `use Hutch`, this macro defines:
    * `rabbit_url/0` - Returns the RabbitMQ connection URL.
    * `prefix/0` - Returns the configured global prefix for queues and exchanges
    * `create_queue/2` - A function to create RabbitMQ queues with retry and DLQ support.

  ## Options
    * `:rabbit_url` (String.t(), required) - The RabbitMQ connection URL for your RabbitMQ server.
      (e.g., `"amqp://guest:guest@localhost"`)
    * `:prefix` (String.t(), required) - A global prefix for queues and exchanges, useful for namespacing.
  """

  require Logger

  # Defaults
  # Default DLQ TTL 14 days
  @hutch_default_dlq_ttl :timer.hours(24) * 14
  @hutch_default_attempts 10
  @hutch_default_retry_interval :timer.minutes(2)

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      require Logger

      # Required options
      @rabbit_url Keyword.fetch!(opts, :rabbit_url)
      @prefix Keyword.fetch!(opts, :prefix)

      @doc "Returns the RabbitMQ connection URL configured for this Hutch instance.     "
      @spec rabbit_url() :: String.t()
      def rabbit_url, do: @rabbit_url

      @doc "Returns the global prefix for queues and exchanges configured for this Hutch instance."
      @spec prefix() :: String.t()
      def prefix, do: @prefix

      @doc """
      Declares a RabbitMQ queue and its associated topology (e.g., retry queues, rejected queues).

      This function simplifies the setup of robust messaging infrastructure. Based on the
      provided options, it can declare:alarm_handler
        - The main process queue
        - A "rejected" queue (acting as a Dead Letter Queue) for messages that ultimately fail
        - Retry queues with message TTLs for delayed retries if `:retry` is enabled

      The names of these queues and exchanges will be prefixed with the global `prefix/0`
      amd the provided `queue_name` (which typically corresponds to a routing key).

      ## Options
        * `:exchange` (String.t(), required) - The RabbitMQ exchange to bind the queue to.
        * `:retry` (boolean(), optional) - Whether to enable retry logic (defaults to `false`).
        * `:retry_attempts` (integer(), optional) - Number of retry attempts before sending to DLQ.
          Defaults to 10.
        * `:retry_interval` (integer(), optional) - Interval between retries in milliseconds.
          Defaults to 2 minutes.
        * `:dlq_ttl` (integer(), optional) - Time-to-live for dead-lettered messages, defaults to 14 days.
        * `:durable` (boolean(), optional) - Whether the queue should be durable (defaults to `true`).
        * `:conn` (String.t(), required in opts for `Hutch.do_create_queue/2`) - The RabbitMQ connection URL.
          Typically provided by `rabbit_url/0` when called from a module that `use Hutch`.

      ## Returns
        * `:ok` if all declarations and bindings are successful.
        * `{:error, reason}` if any operation fails.
      """
      @spec create_queue(String.t(), Keyword.t()) :: :ok | {:error, any()}
      def create_queue(queue_name, opts) do
        Hutch.do_create_queue(queue_name, opts)
      end
    end
  end

  @doc false
  @spec do_create_queue(String.t(), Keyword.t()) :: :ok | {:error, any()}
  def do_create_queue(queue_name, opts) do
    with_channel(opts[:conn], fn channel ->
      do_create_queue(channel, queue_name, opts)
    end)
  end

  @doc false
  @spec do_create_queue(AMQP.Channel.t(), String.t(), Keyword.t()) :: :ok | {:error, any()}
  def do_create_queue(channel, queue_name, opts) do
    config = build_config(queue_name, opts)
    retry = Keyword.get(opts, :retry, false)

    if retry do
      declare_retry_queues(channel, config)

      bindings = [
        binding(config.exchange, config.final_queue_name, config.queue_name),
        binding(config.exchange, config.retry_queue, config.retry_routing_key)
      ]

      bind_queues(channel, bindings)
    else
      declare_base_queue(
        channel,
        config.final_queue_name,
        config.rejected_queue,
        config.durable
      )

      bindings = [
        binding(config.exchange, config.final_queue_name, config.queue_name)
      ]

      bind_queues(channel, bindings)
    end

    declare_rejected_queue(channel, config.rejected_queue, config.dlq_ttl, config.durable)
  end

  defp declare_retry_queues(channel, config) do
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
  end

  defp binding(exchange, queue, routing_key),
    do: [exchange: exchange, queue_name: queue, routing_key: routing_key]

  defp build_config(queue_name, opts) do
    Logger.info("Queue name: #{queue_name}")
    Logger.info("Options: #{inspect(opts)}")
    exchange = Keyword.fetch!(opts, :exchange)
    dlq_ttl = Keyword.get(opts, :dlq_ttl) || @hutch_default_dlq_ttl
    retry_attempts = Keyword.get(opts, :retry_attempts) || @hutch_default_attempts
    retry_interval = Keyword.get(opts, :retry_interval) || @hutch_default_retry_interval
    durable = Keyword.get(opts, :durable, true)
    prefix = Keyword.get(opts, :prefix) || ""

    final_queue_name = prefix <> "." <> queue_name
    rejected_queue = final_queue_name <> ".rejected"
    rejected_exchange = exchange <> ".rejected"
    rejected_routing_key = queue_name <> ".rejected"
    retry_queue = final_queue_name <> ".retry"
    retry_exchange = exchange <> ".retry"
    retry_routing_key = queue_name <> ".retry"

    %{
      exchange: exchange,
      durable: durable,
      retry_attempts: retry_attempts,
      retry_interval: retry_interval,
      dlq_ttl: dlq_ttl,
      queue_name: queue_name,
      prefix: prefix,
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

  defp with_channel(conn, fun) do
    {:ok, connection} = AMQP.Connection.open(conn)
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
        {"x-overflow", :longstr, "reject-publish"},
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
