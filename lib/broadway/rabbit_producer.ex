defmodule Hutch.Broadway.RabbitProducer do
  @moduledoc """
  Provides a reusable Broadway producer setup for consuming messages from RabbitMQ.

  This module is intended to be `use`d within your own Broadway modules.
  It configures a `BroadwayRabbitMQ.Producer` and integrates with Hutch's
  queue management and retry mechanisms. It handles message deserialization
  (defaulting to JSON) and injects a custom acknowledger for retry logic
  when enabled.

  ## Example Usage

  ```elixir
  defmodule MyApp.MyConsumer do
    use Hutch.Broadway.RabbitProducer,
      queue_manager: MyApp.QueueManager,
      exchange: "my_exchange",
      routing_key: "my_routing_key",
      retry: true,
      # other Broadway options...

    @impl true
    def handle_message(_processor, message, _context) do
      # Process your message
      {:ok, message}
    end

    # Optionally, you can override the `decode_payload/1` function
    # def decode_payload(message) do
    #   # Custom decoding logic if needed
    #   message
    # end
  end
  ```

  ## Options

  This macro accepts several options, many of which are passed to Broadway
  or `BroadwayRabbitMQ.Producer`:

    * `:queue_manager` (atom(), required) - The module that manages queue configurations
      and provides `rabbit_url/0` and `prefix/0` functions (typically your `Hutch` module).
    * `:exchange` (String.t(), required) - The RabbitMQ exchange to consume from.
    * `:routing_key` (String.t(), required) - The routing key for the queue binding.
    * `:name` (atom(), optional) - The name of the Broadway module (defaults to the module name).
    * `:batchers` (Keyword.t(), optional) - List of batchers to use in Broadway. Defaults to `[]`
    * `:durable` (boolean(), optional) - Whether the queue should be durable (defaults to `true`).
    * `:dlq_ttl` (integer(), optional) - Time-to-live for dead-lettered messages,
      used by the `:queue_manager` when creating the queue.
    * `:prefetch_count` (integer(), optional) - RabbitMQ prefetch count (Qos). Defaults to `20`.
    * `:prefix` (String.t(), optional) - Prefix for the queue name, used to avoid conflicts
      with other queues (defaults to the prefix from the `:queue_manager`).
    * `:processors` (Keyword.t(), optional) - List of processors to use in Broadway (defaults to `[]`).
    * `:retry` (boolean(), optional) - Whether to enable retry logic (defaults to `false`).
    * `:retry_attempts` (integer(), optional) - Number of retry attempts before sending to DLQ. Used by
      the `:queue_manager` and `Hutch.RetryAcknowledger`.
    * `:retry_interval` (integer(), optional) - Interval between retries in milliseconds (defaults to 2 minutes).
      Used by the `:queue_manager`.
    * `:worker_count` (integer(), optional) - Number of concurrent producer workers. Defaults to `2`.
    * `:partitioned_by` (atom() | {module, function, args}, optional) - Broadway partitioning configuration.
      If provided, messages will be partitioned accordingly.
  """
  alias Broadway.Message

  require Logger

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      require Logger

      @queue_manager Keyword.fetch!(opts, :queue_manager)

      @batchers Keyword.get(opts, :batchers, [])
      @default_retry_interval :timer.minutes(2)
      @dlq_ttl Keyword.get(opts, :dlq_ttl)
      @durable Keyword.get(opts, :durable, true)
      @exchange Keyword.fetch!(opts, :exchange)
      @name Keyword.get(opts, :name, __MODULE__)
      @prefetch_count Keyword.get(opts, :prefetch_count, 20)
      @prefix Keyword.get(opts, :prefix, @queue_manager.prefix())
      @processors Keyword.get(opts, :processors, default: [])
      @retry Keyword.get(opts, :retry, false)
      @retry_attempts Keyword.get(opts, :retry_attempts)
      @retry_interval Keyword.get(opts, :retry_interval)
      @routing_key Keyword.fetch!(opts, :routing_key)
      @worker_count Keyword.get(opts, :worker_count, 2)

      use Broadway

      @doc """
      Starts the Broadway topology for this RabbitMQ producer.

      This function is typically called by a supervisor. It initializes the
      RabbitMQ producer and the Broadway processing pipeline based on the
      configuration provided via `use Hutch.Broadway.RabbitProducer`.
      It also ensures that the necessary RabbitMQ queue and exchange topology
      are created via the configured `@queue_manager`.
      """
      @spec start_link(term()) :: Supervisor.on_start()
      def start_link(_opts) do
        producer = [
          module:
            {BroadwayRabbitMQ.Producer,
             connection: @queue_manager.rabbit_url(),
             queue: "#{@prefix}.#{@routing_key}",
             qos: [prefetch_count: @prefetch_count],
             on_failure: :reject,
             metadata: [
               :delivery_tag,
               :headers
             ]},
          concurrency: @worker_count
        ]

        @queue_manager.create_queue(@routing_key,
          conn: @queue_manager.rabbit_url(),
          exchange: @exchange,
          dlq_ttl: @dlq_ttl,
          durable: @durable,
          retry: @retry,
          retry_attempts: @retry_attempts,
          retry_interval: @retry_interval,
          prefix: @prefix
        )

        Broadway.start_link(
          __MODULE__,
          with_partition_by(
            name: @name,
            producer: producer,
            processors: @processors,
            batchers: @batchers
          )
        )
      end

      if @retry do
        @doc """
        Prepares messages before they are processed by `handle_message/3`.

        When retry is enabled, this function injects the `Hutch.RetryAcknowledger`
        into each message. This custom acknowledger handles retry logic
        and ensures messages are requeued or sent to a dead-letter queue after too many failures.

        This callback is part of the Broadway behaviour.
        """
        @impl true
        def prepare_messages(messages, context) do
          messages
          |> Enum.map(fn msg ->
            msg
            |> inject_acknowledger()
          end)
        end
      end

      defp inject_acknowledger(msg) do
        # Inject the ack_ref and ack_data into the message
        retry_config = %{
          retry_attempts: @retry_attempts,
          queue_name: @routing_key,
          exchange: @exchange,
          prefix: @prefix,
          conn: @queue_manager.rabbit_url()
        }

        %{
          msg
          | acknowledger: {Hutch.RetryAcknowledger, retry_config, msg.acknowledger}
        }
      end

      @doc """
      Decodes the payload of an incoming RabbitMQ message.

      By default, this function attempts to decode the `msg.data` field as JSON.
      If decoding is successful, the `data` field is updated with the parsed JSON.
      If decoding fails, the message is marked as failed.

      This function is overridable. You can define your own `decode_payload/1`
      in your consumer module if you need custom decoding logic.

      ## Example of overriding

      ```elixir
      defmodule MyApp.MyConsumer do
        use Hutch.Broadway.RabbitProducer, # other options...

        # ...

        @impl Hutch.Broadway.RabbitProducer
        def decode_payload(msg) do
          # Custom decoding logic here
          msg
        end
      end
      ```
      """
      @spec decode_payload(Message.t()) :: Message.t()
      def decode_payload(msg) do
        case Jason.decode(msg.data) do
          {:ok, json} ->
            Message.update_data(msg, fn _ -> json end)

          {:error, _} ->
            err_msg = "Error decoding msg: #{msg.data}"
            Logger.error(err_msg)
            Message.failed(msg, err_msg)
        end
      end

      defoverridable decode_payload: 1

      defp with_partition_by(args) do
        partition_by = unquote(opts[:partitioned_by])
        if is_nil(partition_by), do: args, else: [{:partition_by, partition_by} | args]
      end
    end
  end
end
