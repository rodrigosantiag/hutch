defmodule Hutch.RetryAcknowledger do
  @moduledoc """
  A `Broadway.Acknowledger` for implementing message retry and dead-lettering logic.

  This acknowledger is used by `Hutch.Broadway.RabbitProducer` when the `:retry`
  option is enabled. It handles the acknowledgment of messages processed by
  Broadway, deciding whether to:
  1. Simply ACK successful messages.
  2. Reject failed messages and requeue them if they haven't exceeded the retry limit.
  3. Send failed messages to a rejected queue if they have exceeded the retry limit.

  It relies on message headers (specifically the `x-death` header) to count retry attempts
  and configuration passed during its setup (max retry attempts, queue names, etc.).
  """
  @behaviour Broadway.Acknowledger

  require Logger

  @doc """
  Configures the acknowledger with data needed for retry logic.

  This is a `Broadway.Acknowledger` callback. The `ack_data` is expected to be
  a map containing retry configuration like `:retry_attempts`, `:queue_name`,
  `:exchange`, `:prefix`, and `:conn` (RabbitMQ connection URL for the publisher).
  """
  @impl true
  def configure(_original_ack_ref, ack_data, _options) do
    {:ok, ack_data}
  end

  @doc """
  Handles acknowledgment for a batch of processed messages.

  This is the core logic of the `Broadway.Acknowledger` behaviour.

  For each `successful` message:
    - It extracts the original AMQP channel and delivery tag.
    - It sends an `AMQP.Basic.ack` to RabbitMQ.

  For each `failed` message:
    - It extracts the original AMQP channel, delivery tag, and message headers.
    - It counts the number of retry attempts using the "x-death" headers.
    - If `attempts_count < max_retries` (from `ack_ref` which is the `retry_config`):
      - It rejects the message using `AMQP.Basic.reject(channel, delivery_tag, requeue: false)`.
        This relies on the source queue being configured with a dead-letter exchange
        pointing to a retry queue.
    - If `attempts_count >= max_retries`:
      - It logs that the message is being sent to the rejected queue.
      - It constructs the name of the rejected queue (e.g., `prefix.queue_name.rejected`).
      - It ensures the `Hutch.Publisher` is started.
      - It publishes the failed message's data (as JSON) to the rejected queue using `Hutch.Publisher.publish/4`.
      - It then ACKs the original message (`AMQP.Basic.ack`) to remove it from the processing queue.
        This is crucial because the message has now been successfully "handled" by moving it to the rejected queue.
  """
  @impl true
  def ack(ack_ref, successful, failed) do
    Logger.info("Acknowledging ack_ref: #{inspect(ack_ref)}")

    Enum.each(failed, fn message ->
      {_, _, original_ack_data} = message.acknowledger

      # Extract the channel and delivery_tag from the original ack data
      {_, channel, _} = original_ack_data

      delivery_tag = message.metadata[:delivery_tag]

      headers = message.metadata[:headers]

      max_retries = ack_ref.retry_attempts

      attempts_count = count_retries_from_headers(headers)

      if attempts_count < max_retries do
        Logger.info("Message failed, requeueing: #{inspect(message.data)}")

        AMQP.Basic.reject(
          channel,
          delivery_tag,
          requeue: false
        )
      else
        Logger.info("Message failed, sending to rejected queue: #{inspect(message.data)}")

        rejected_queue =
          ack_ref.prefix <> "." <> ack_ref.queue_name <> ".rejected"

        Logger.info("Rejected queue: #{inspect(rejected_queue)}")
        Logger.info("Message ack_ref: #{inspect(ack_ref)}")

        ensure_publisher_started(ack_ref.conn)

        payload = Jason.encode!(message.data)

        # TODO: refactor this (too nested)
        case Hutch.Publisher.publish(
               "",
               rejected_queue,
               payload,
               persistent: true,
               headers: message.metadata[:headers]
             ) do
          :ok ->
            Logger.info("Message sent to rejected queue successfully")

          {:error, reason} ->
            Logger.error("Failed to send message to rejected queue: #{inspect(reason)}")
        end

        AMQP.Basic.ack(channel, delivery_tag)
      end
    end)

    Enum.each(successful, fn message ->
      # Extract the nested structure correctly
      {_, _, original_ack_data} = message.acknowledger

      # Extract the channel and delivery_tag from the original ack data
      {_, channel, _} = original_ack_data

      # Get the delivery tag from the options
      delivery_tag = message.metadata[:delivery_tag]

      # Call AMQP.Basic.ack directly with the correct parameters
      AMQP.Basic.ack(channel, delivery_tag)
    end)
  end

  defp count_retries_from_headers(headers) when is_list(headers) do
    case List.keyfind(headers, "x-death", 0) do
      {"x-death", :array, death_entries} ->
        count_from_death_entries(death_entries)

      _ ->
        0
    end
  end

  defp count_retries_from_headers(_headers), do: 0

  defp count_from_death_entries(death_entries) do
    Enum.reduce(death_entries, 0, fn
      {:table, properties}, acc ->
        case List.keyfind(properties, "reason", 0) do
          {"reason", :longstr, "rejected"} ->
            # TODO: refactor this (too nested)
            case List.keyfind(properties, "count", 0) do
              {"count", :long, count} -> acc + count
              _ -> acc
            end

          _ ->
            acc
        end

      _, acc ->
        acc
    end)
  end

  defp ensure_publisher_started(conn) do
    case Process.whereis(Hutch.Publisher) do
      nil ->
        Hutch.Publisher.start_link(rabbit_url: conn)

      _pid ->
        :ok
    end
  end
end
