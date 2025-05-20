defmodule Hutch.Broadway.RabbitProducerTest do
  @moduledoc false

  use ExUnit.Case, async: false
  use Mimic

  alias Broadway.Message

  defmodule TestProducer do
    use Hutch.Broadway.RabbitProducer,
      queue_manager: TestHelpers.MockQueueManager,
      exchange: "test_exchange",
      routing_key: "test_key",
      retry: true

    def handle_message(_, message, _) do
      message
    end
  end

  setup :set_mimic_global

  describe "decode_payload/1" do
    test "returns message with decoded JSON on success" do
      message = %Message{acknowledger: {:default, %{}, %{}}, data: ~s({"foo": "bar"})}
      result = TestProducer.decode_payload(message)
      assert result.data == %{"foo" => "bar"}
    end

    test "returns failed message on JSON error" do
      message = %Message{acknowledger: {:default, %{}, %{}}, data: "not-json"}
      result = TestProducer.decode_payload(message)
      assert result.status == {:failed, "Error decoding msg: not-json"}
    end
  end

  describe "start_link/1" do
    test "calls queue manager and starts Broadway" do
      expect(TestHelpers.MockQueueManager, :create_queue, fn _, _ -> :ok end)
      expect(Broadway, :start_link, fn _, _ -> {:ok, self()} end)

      assert {:ok, _pid} = TestProducer.start_link([])
    end
  end
end
