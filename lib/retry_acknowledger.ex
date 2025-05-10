defmodule Hutch.RetryAcknowledger do
  @behaviour Broadway.Acknowledger

  require Logger

  @impl true
  def configure(_original_ack_ref, ack_data, _options) do
    {:ok, ack_data}
  end

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

        # publisher_options = [
        #   content_type: message.metadata[:content_type],
        #   content_encoding: message.metadata[:content_encoding],
        #   headers: add_rejection_headers(headers, attempts_count),
        #   persistent: true
        # ]

        payload = Jason.encode!(message.data)

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
            # Found a rejection entry, get the count
            # TODO: refactor this
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

  defp add_rejection_headers(headers, attempts_count) do
    rejection_headers = [
      {"x-rejected", :bool, true},
      {"x-rejection-timestamp", :timestamp, :os.system_time(:seconds)},
      {"x-retry-count", :long, attempts_count}
    ]

    # Combine with existing headers
    (headers || []) ++ rejection_headers
  end
end
