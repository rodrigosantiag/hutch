defmodule Hutch.Broadway.Producer do
  alias Broadway.Message

  require Logger

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      require Logger

      @batchers Keyword.get(opts, :batchers, [])
      @default_retry_interval :timer.minutes(2)
      @default_dlq_ttl :timer.hours(24) * 14 # 14 days
      @durable Keyword.get(opts, :durable, true)
      @exchange Keyword.fetch!(opts, :exchange)
      @name Keyword.get(opts, :name, __MODULE__)
      @prefetch_count Keyword.get(opts, :prefetch_count, 20)
      @prefix Keyword.get(opts, :prefix, "hutch")
      @processors Keyword.get(opts, :processors, default: [])
      @retry Keyword.get(opts, :retry, false)
      @rabbit_url Hutch.rabbit_url()
      @retry_interval Keyword.get(opts, :retry_interval, @default_ttl_ms)
      @routing_key Keyword.fetch!(opts, :routing_key)
      @ttl Keyword.get(opts, :ttl, @default_dlq_ttl)
      @worker_count Keyword.get(opts, :worker_count, 2)

      use Broadway

      @spec start_link(term()) :: Supervisor.on_start()
      def start_link(_opts) do
        producer = [
          module:
            {BroadwayRabbitMQ.Producer,
             connection: @rabbit_url,
             queue: "#{@prefix}.#{@routing_key}",
             qos: [prefetch_count: @prefetch_count],
             on_failure: :reject},
          concurrency: @worker_count
        ]

        Hutch.create_queue(@routing_key,
          exchange: @exchange,
          ttl: @ttl,
          durable: @durable,
          retry: @retry
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
