defmodule Hutch.Broadway.RabbitProducer do
  alias Broadway.Message

  require Logger

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      require Logger

      @queue_manager Keyword.fetch!(opts, :queue_manager)
      @batchers Keyword.get(opts, :batchers, [])
      @default_retry_interval :timer.minutes(2)
      # 14 days
      @default_dlq_ttl :timer.hours(24) * 14
      @durable Keyword.get(opts, :durable, true)
      @exchange Keyword.fetch!(opts, :exchange)
      @name Keyword.get(opts, :name, __MODULE__)
      @prefetch_count Keyword.get(opts, :prefetch_count, 20)
      @prefix Keyword.get(opts, :prefix, @queue_manager.prefix())
      @processors Keyword.get(opts, :processors, default: [])
      @retry Keyword.get(opts, :retry, false)
      @retry_interval Keyword.get(opts, :retry_interval, @default_retry_interval)
      @routing_key Keyword.get(opts, :routing_key)
      @ttl Keyword.get(opts, :ttl, @queue_manager.default_dlq_ttl())
      @worker_count Keyword.get(opts, :worker_count, 2)

      use Broadway

      @spec start_link(term()) :: Supervisor.on_start()
      def start_link(_opts) do
        producer = [
          module:
            {BroadwayRabbitMQ.Producer,
             connection: @queue_manager.rabbit_url(),
             queue: "#{@prefix}.#{@routing_key}",
             qos: [prefetch_count: @prefetch_count],
             on_failure: :reject},
          concurrency: @worker_count
        ]

        Hutch.create_queue(@routing_key,
          exchange: @exchange,
          ttl: @ttl,
          durable: @durable,
          rabbit_url: @queue_manager.rabbit_url(),
          retry: @retry,
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
