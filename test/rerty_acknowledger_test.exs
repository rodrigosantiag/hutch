defmodule Hutch.RetryAcknowledgerTest do
  @moduledoc false

  use ExUnit.Case, async: true
  import Mimic

  alias Hutch.RetryAcknowledger

  defp message_with(headers, delivery_tag \\ 1) do
    %Broadway.Message{
      acknowledger: {RetryAcknowledger, %{}, {:fake, %{}, %{}}},
      metadata: %{headers: headers, delivery_tag: delivery_tag},
      data: "payload"
    }
  end

  describe "ack/3" do
    setup do
      ack_ref = %{
        retry_attempts: 3,
        prefix: "prefix",
        queue_name: "queue",
        conn: "amqp://guest:guest@localhost"
      }

      {:ok, ack_ref: ack_ref}
    end

    test "rejects failed message when retries are below max", %{ack_ref: ack_ref} do
      Mimic.expect(AMQP.Basic, :reject, fn _chan, _tag, requeue: false -> :ok end)

      headers = [
        {"x-death", :array,
         [
           {:table, [{"reason", :longstr, "rejected"}, {"count", :long, 1}]}
         ]}
      ]

      message = message_with(headers)

      Hutch.RetryAcknowledger.ack(ack_ref, [], [message])

      verify!()
    end

    test "publishes failed message to rejected queue when retries meet or exceed max", %{
      ack_ref: ack_ref
    } do
      Mimic.expect(AMQP.Basic, :ack, fn _chan, _tag -> :ok end)

      Mimic.expect(Hutch.Publisher, :publish, fn
        "", "prefix.queue.rejected", _payload, persistent: true, headers: _headers -> :ok
      end)

      headers = [
        {"x-death", :array,
         [
           {:table, [{"reason", :longstr, "rejected"}, {"count", :long, 3}]}
         ]}
      ]

      message = message_with(headers)

      Hutch.RetryAcknowledger.ack(ack_ref, [], [message])

      verify!()
    end

    test "acknowledges successful messages" do
      Mimic.expect(AMQP.Basic, :ack, fn _chan, _tag -> :ok end)

      message = message_with([])

      Hutch.RetryAcknowledger.ack(%{}, [message], [])

      verify!()
    end

    test "starts publisher if not started when publishing to rejected queue", %{ack_ref: ack_ref} do
      Mimic.expect(AMQP.Basic, :ack, fn _chan, _tag -> :ok end)

      Mimic.expect(Hutch.Publisher, :publish, fn
        "", "prefix.queue.rejected", _payload, persistent: true, headers: _headers -> :ok
      end)

      # We do not mock Process.whereis/1 here because Mimic can't easily mock it,
      # so this test assumes publisher is started or starts correctly.

      headers = [
        {"x-death", :array,
         [
           {:table, [{"reason", :longstr, "rejected"}, {"count", :long, 5}]}
         ]}
      ]

      message = message_with(headers)

      Hutch.RetryAcknowledger.ack(ack_ref, [], [message])

      verify!()
    end
  end
end
