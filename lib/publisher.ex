defmodule Hutch.Publisher do
  @moduledoc """
  A module that provides a simple interface for publishing messages to RabbitMQ.
  """

  use GenServer

  require Logger

  # Server API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    rabbit_url = Keyword.fetch!(opts, :rabbit_url)

    if is_binary(rabbit_url) and rabbit_url != "" do
      {:ok, %{conn: nil, channel: nil, ref: nil, url: rabbit_url}, {:continue, :connect}}
    else
      {:stop, {:error, :invalid_rabbit_url}}
    end
  end

  @impl true
  def handle_continue(:connect, state) do
    connect_and_update_state(state)
  end

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

  @impl true
  def handle_info(:connect, state) do
    connect_and_update_state(state)
  end

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

  @impl true
  def terminate(_reason, state) do
    safely_close_channel(state.channel)
    safely_close_connection(state.conn)
    :ok
  end

  # Public API
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
