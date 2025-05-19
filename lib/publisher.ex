defmodule Hutch.Publisher do
  @moduledoc """
  A GenServer for publishing messages to RabbitMQ reliably.

  Ths module manages a persistent connection to RabbitMQ and provides a simple
  API to publish messages. It handles connection establishment, channel opening,
  and attempts to reconnect if the connection is lost.

  ## Example Usage

  # To start the publisher (e.g., in your `application.ex`):
  ```elixir
  children = [
    {Hutch.Publisher, rabbit_url: "amqp://guest:guest@localhost"}
    # Other children...
  ]
  Supervisor.start_link(children, strategy: :one_for_one)
  ```

  # To publish a message:
  ```elixir
  Hutch.Publisher.publish("my_exchange", "my_routing_key", "Hello, RabbitMQ!")
  # With options
  Hutch.Publisher.publish("my_exchange", "my_routing_key", "Hello, RabbitMQ!", persistent: true, content_type: "application/json")
  ```
  """

  use GenServer

  require Logger

  # Server API

  @doc """
  Starts the `Hutch.Publisher` GenServer.

  This is typically called by a supervisor.

  ## Options

    * `:rabbit_url` (String.t(), required) - The RabbitMQ connection URL.
      (e.g., `"amqp://guest:guest@localhost"`)
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Initializes the `Hutch.Publisher` GenServer.

  Sets up the initial state with the RabbitMQ connection URL and triggers connection.

  This is an internal GenServer callback.
  """
  @impl true
  def init(opts) do
    rabbit_url = Keyword.fetch!(opts, :rabbit_url)

    if is_binary(rabbit_url) and rabbit_url != "" do
      {:ok, %{conn: nil, channel: nil, ref: nil, url: rabbit_url}, {:continue, :connect}}
    else
      {:stop, {:error, :invalid_rabbit_url}}
    end
  end

  @doc """
  Handles the deferred connection attempt after init.

  This is an internal GenServer callback.
  """
  @impl true
  def handle_continue(:connect, state) do
    connect_and_update_state(state)
  end

  @doc """
  Handles publish requests.

  Publishes the message using `AMQP.Basic.publish/5`.

  This is an internal GenServer callback.
  """
  @impl true
  def handle_call(
        {:publish, _exchange, _routing_key, _payload, _options},
        _from,
        %{channel: nil} = state
      ) do
    {:reply, {:error, :not_connected}, state}
end

  @impl true
  def handle_call(
        {:publish, exchange, routing_key, payload, options},
        _from,
        %{channel: channel} = state
      ) do
    result = AMQP.Basic.publish(channel, exchange, routing_key, payload, options)
    {:reply, result, state}
  end

  @doc """
  Handles explicit connection requests or scheduled connections.

  This is an internal GenServer callback.
  """
  @impl true
  def handle_info(:connect, state) do
    connect_and_update_state(state)
  end

  @doc """
  Handles connection loss notifications.

  When the RabbitMQ connection is lost, this callback is triggered.
  It attempts to close the channel and schedules a reconnection after a delay.

  This is an internal GenServer callback.
  """
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{ref: ref} = state) do
    Logger.error("Connection to RabbitMQ lost: #{inspect(reason)}")

    if state.channel, do: safely_close_channel(state.channel)

    Process.send_after(self(), :connect, 5000)
    {:noreply, %{state | conn: nil, channel: nil, ref: nil}}
end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  @doc """
  Cleans up resources when the GenServer is terminated.

  This is an internal GenServer callback.
  """
  @impl true
  def terminate(_reason, state) do
    safely_close_channel(state.channel)
    safely_close_connection(state.conn)
    :ok
  end

  # Public API

  @doc """
  Publishes a message to the specified RabbitMQ exchange with the given routing key and payload.

  This is a primary public interface for sending messages. It's a synchronous
  call to the `Hutch.Publisher` GenServer.

  ## Parameters

    * `exchange` (String.t()) - The name of the exchange to publish to.
    * `routing_key` (String.t()) - The routing key for the message.
    * `payload` (String.t() | binary()) - The message payload.
    * `options` (Keyword.t(), optional) - Options for `AMQP.Basic.publish/5`.
      Common options include:
        - `persistent` (boolean()): Marks message as persistent.
        - `content_type` (String.t()): The MIME type of the message content (e.g., "application/json").
        - `headers`: Additional headers for the message.

  ## Returns

    * `:ok` if the message was successfully published.
    * `{:error, :not_connected}` if the publisher is not connected to RabbitMQ.
    * Other error tuples from `AMQP.Basic.publish/5` if the publish fails.
  """
  def publish(exchange, routing_key, payload, options \\ []) do
    GenServer.call(__MODULE__, {:publish, exchange, routing_key, payload, options})
  end

  defp connect_and_update_state(%{url: url} = state) do
    case connect(url) do
      {:ok, conn, channel} ->
        ref = Process.monitor(conn.pid)
        {:noreply, %{state | conn: conn, channel: channel, ref: ref}}

      {:error, reason} ->
        Logger.error("Failed to connect to RabbitMQ: #{inspect(reason)}")
        Process.send_after(self(), :connect, 5000)
        {:noreply, state}
    end
  end

  defp connect(url) do
    with {:ok, conn} <- AMQP.Connection.open(url),
         {:ok, channel} <- AMQP.Channel.open(conn) do
      :ok = AMQP.Confirm.select(channel)
      {:ok, conn, channel}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  defp safely_close_channel(nil), do: :ok

  defp safely_close_channel(channel) do
    try do
      if is_pid(channel.pid) and Process.alive?(channel.pid) do
        AMQP.Channel.close(channel)
      end
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    :ok
  end

  defp safely_close_connection(nil), do: :ok

  defp safely_close_connection(conn) do
    try do
      if is_pid(conn.pid) and Process.alive?(conn.pid) do
        AMQP.Connection.close(conn)
      end
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    :ok
  end
end
