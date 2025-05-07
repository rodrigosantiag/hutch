defmodule Hutch.RetryAcknowledger do
  @behaviour Broadway.Acknowledger

  require Logger

  @impl true
  def configure(original_ack_ref, ack_data, options) do
    Logger.info("Original ack_ref: #{inspect(original_ack_ref)}")
    Logger.info("Ack data: #{inspect(ack_data)}")
    Logger.info("Options: #{inspect(options)}")

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
        Logger.info("Message failed, sending to dead letter queue: #{inspect(message.data)}")

        rejected_queue =
          ack_ref.prefix <> "." <> ack_ref.queue_name <> ".rejected"

        Logger.info("Rejected queue: #{inspect(rejected_queue)}")
        Logger.info("Message ack_ref: #{inspect(ack_ref)}")

        # TODO: find a way to publish to the rejected queue...
        # it looks like the channel is not available here
        # Maybe create a new channel for publishing?
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

  defp count_retries_from_headers(headers) do
    case List.keyfind(headers, "x-death", 0) do
      {"x-death", :array, death_entries} ->
        count_from_death_entries(death_entries)

      _ ->
        0
    end
  end

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
end
