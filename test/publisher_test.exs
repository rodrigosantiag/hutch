defmodule Hutch.PublisherTest do
  @moduledoc false

  use ExUnit.Case, async: false
  use Mimic

  alias Hutch.Publisher

  setup :verify_on_exit!
  setup :set_mimic_global

  describe "connection" do
    test "connects on init" do
      expect(AMQP.Connection, :open, fn _ -> {:ok, %{pid: self()}} end)
      expect(AMQP.Channel, :open, fn _ -> {:ok, %{pid: self()}} end)
      expect(AMQP.Confirm, :select, fn _ -> :ok end)

      {:ok, _pid} = Publisher.start_link(rabbit_url: "amqp://guest:guest@localhost")
      Process.sleep(100)
    end

    test "reconnects on connection down" do
      test_pid = self()

      conn_pid =
        spawn(fn ->
          send(test_pid, {:connection_created, self()})
          Process.sleep(1000)
        end)

      expect(AMQP.Connection, :open, 2, fn _ ->
        {:ok, %{pid: conn_pid}}
      end)

      expect(AMQP.Channel, :open, 2, fn _ ->
        {:ok, %{pid: self()}}
      end)

      expect(AMQP.Confirm, :select, 2, fn _ -> :ok end)
      stub(AMQP.Channel, :close, fn _ -> :ok end)
      stub(AMQP.Connection, :close, fn _ -> :ok end)

      {:ok, _pid} = Publisher.start_link(rabbit_url: "amqp://guest:guest@localhost")

      receive do
        {:connection_created, pid} ->
          Process.exit(pid, :kill)
          Process.sleep(5100)
      after
        1000 -> flunk("Connection not created")
      end
    end
  end

  describe "publish" do
    test "successfully publishes message" do
      expect(AMQP.Connection, :open, fn _ -> {:ok, %{pid: self()}} end)
      expect(AMQP.Channel, :open, fn _ -> {:ok, %{pid: self()}} end)
      expect(AMQP.Confirm, :select, fn _ -> :ok end)

      expect(AMQP.Basic, :publish, fn _ch, "test_exchange", "test_key", "test_payload", _opts ->
        :ok
      end)

      {:ok, _pid} = Publisher.start_link(rabbit_url: "amqp://guest:guest@localhost")
      Process.sleep(100)

      assert :ok = Publisher.publish("test_exchange", "test_key", "test_payload")
    end

    test "returns error when not connected" do
      expect(AMQP.Connection, :open, fn _ -> {:error, :test_error} end)

      {:ok, _pid} = Publisher.start_link(rabbit_url: "amqp://guest:guest@localhost")
      Process.sleep(100)

      assert {:error, :not_connected} =
               Publisher.publish("test_exchange", "test_key", "test_payload")
    end
  end
end
