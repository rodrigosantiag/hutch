# Hutch

Hutch is an Elixir library laser-focused on simplifying robust message consumption from RabbitMQ by providing automated retry and dead-letter queue (DLQ) management. It leverages [Broadway](https://hexdocs.pm/broadway/Broadway.html) for efficient data processing pipelines.

## Features

* **Automated Retry & DLQ Topology:** Queues, exchanges, and bindings for handling message retries and dead-lettering are automatically managed based on your consumer configurations.
* **Broadway Integration:** Seamlessly create RabbitMQ consumers as Broadway producers, with Hutch handling the complexities of fault tolerance.
* **Configurable Retry Mechanism:** Automatic message retries with configurable attempts and intervals, using dedicated retry queues and RabbitMQ's dead-lettering capabilities.
* **Dead-Letter Queue (DLQ) Handling:** Failed messages, after exhausting retries, are automatically routed to a "rejected" queue for inspection or further processing.
* **Centralized Configuration:** Define RabbitMQ connection details and global prefixes in a dedicated configuration module.
* **JSON Decoding by Default:** Incoming messages are automatically decoded from JSON, with the option to override for custom deserialization.
* **Internal Publisher for DLQ:** Includes a resilient internal publisher used by the retry mechanism to route messages to the rejected queue.

## Installation

Add `hutch` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:hutch, "~> 0.1.0"} # Replace with the desired version
  ]
end
```

Then, fetch the dependencies:

```bash
mix deps.get
```

## Core Concepts & Usage

Hutch is built around a few key modules that you `use` to integrate its functionality:

1.  **`Hutch`**: Defines global Hutch settings like the RabbitMQ URL and queue prefix. It's configured once using `use Hutch, ...` and then referenced by consumers.
2.  **`Hutch.Broadway.RabbitProducer`**: For defining Broadway consumers that process messages from RabbitMQ. Using this module triggers the automatic setup of the required RabbitMQ topology for retries and DLQs.
3.  **`Hutch.RetryAcknowledger`**: An internal Broadway acknowledger that automatically handles the retry and DLQ logic when retries are enabled for a consumer.
4.  **`Hutch.Publisher`**: An internal GenServer primarily used by `Hutch.RetryAcknowledger` to publish messages to the final "rejected" queue. It automatically starts when needed by the retry mechanism.

### 1. Global Configuration (`Hutch`)

First, create a module in your application to centralize Hutch's global settings. This module will `use Hutch` and define your RabbitMQ connection URL and a default prefix for your queues and exchanges.

```elixir
# lib/my_app/hutch.ex
defmodule MyApp.Hutch do
  use Hutch,
    rabbit_url: "amqp://guest:guest@localhost:5672", # Your RabbitMQ URL
    prefix: "my_app" # A default prefix for all queues/exchanges

  # This module serves as the central configuration point for Hutch.
  # It provides the necessary settings (like RabbitMQ URL and default prefix)
  # that Hutch.Broadway.RabbitProducer will use to automatically manage
  # queue topology for your consumers.
end
```

This `MyApp.Hutch` module will then be referenced as the `:queue_manager` when you define your consumers.

### 2. Consuming Messages with Retry/DLQ (`Hutch.Broadway.RabbitProducer`)

To consume messages with Hutch's retry and DLQ capabilities, define a Broadway module and `use Hutch.Broadway.RabbitProducer`. This sets up a RabbitMQ producer for your Broadway topology and handles the declaration of the necessary queues and exchanges based on its configuration.

```elixir
# lib/my_app/consumers/user_event_consumer.ex
defmodule MyApp.UserEventConsumer do
  use Hutch.Broadway.RabbitProducer,
    # Reference your Hutch configuration module
    queue_manager: MyApp.Hutch,
    # Define the exchange and routing key this consumer will listen to
    exchange: "events_exchange",
    routing_key: "user_events", # This forms the base name of the queue
    # Enable Hutch's retry mechanism for this consumer
    retry: true,
    retry_attempts: 5,
    retry_interval: :timer.minutes(1),
    # Optional: Broadway specific options
    processors: [default: [concurrency: 10]],
    prefetch_count: 10

  alias Broadway.Message

  # Standard Broadway callback to process messages
  @impl Broadway
  def handle_message(_, %Message{data: event_data} = message, _) do
    IO.inspect(event_data, label: "Processing user event")
    # Your processing logic here...
    # Return {:ok, message} on success.
    # Return {:error, reason, message} or raise an error to trigger Hutch's retry/DLQ mechanism.
    {:ok, message}
  end

  # Optional: Override if your messages are not JSON or need custom decoding
  # @impl Hutch.Broadway.RabbitProducer
  # def decode_payload(message) do
  #   # ... custom decoding logic ...
  #   Message.update_data(message, fn _ -> "decoded_data" end)
  # end
end
```

Remember to add your consumer to your application's supervision tree:

```elixir
# lib/my_app/application.ex
children = [
  # ... other children
  MyApp.UserEventConsumer
]
```

When `MyApp.UserEventConsumer` starts, `Hutch.Broadway.RabbitProducer` will automatically attempt to declare the necessary RabbitMQ queue (e.g., `my_app.user_events`), the exchange (`events_exchange`), and any associated retry/rejected queues because `:retry` is enabled. This process uses the settings from your `MyApp.Hutch` module (specified as `:queue_manager`) and the queue-specific options provided directly in `use Hutch.Broadway.RabbitProducer`.

The `Hutch.Publisher` is managed internally by Hutch. When a message needs to be sent to the rejected queue after exhausting retries, `Hutch.RetryAcknowledger` ensures the publisher is started and uses it for this purpose. You do not need to add `Hutch.Publisher` to your application's supervision tree separately if your sole use of Hutch is for its consumer retry/DLQ features.

## Configuration Details

### `use Hutch` options (for your global Hutch config module):

* `:rabbit_url` (String.t, **required**): The RabbitMQ connection URL.
* `:prefix` (String.t, **required**): A global prefix for all queues and exchanges managed by Hutch. This prefix is used by default when consumers declare queues, unless overridden at the consumer level.

### `use Hutch.Broadway.RabbitProducer` options:

These options configure your message consumer and how it interacts with RabbitMQ, including the automatic setup of queues for retry and dead-lettering.

* `:queue_manager` (atom, **required**): Your Hutch configuration module (e.g., `MyApp.Hutch`). This provides the base RabbitMQ URL and default global prefix.
* `:exchange` (String.t, **required**): The RabbitMQ exchange your consumer's queue will bind to. Hutch will declare this exchange (as durable, topic) if it doesn't exist.
* `:routing_key` (String.t, **required**): The routing key for the queue binding. This also forms the base name of the queue (e.g., `your_prefix.your_routing_key`).
* `:retry` (boolean, optional): Enables Hutch's automatic retry and dead-lettering mechanism for this consumer. Defaults to `false`.
    * When `true`, consider setting `:retry_attempts` and `:retry_interval` if the library defaults are not suitable.
* `:retry_attempts` (integer, optional): Maximum number of retry attempts before a message is sent to the rejected queue. Relevant only if `:retry` is `true`. Defaults to 10 (as defined in Hutch library).
* `:retry_interval` (integer, optional): Base interval in milliseconds for message retries. Messages will be delayed by this duration before being re-processed. Relevant only if `:retry` is `true`. Defaults to 2 minutes (as defined in Hutch library).
* `:dlq_ttl` (integer, optional): Time-to-live (in milliseconds) for messages in the final "rejected" queue (after all retries are exhausted). Defaults to 14 days (as defined in Hutch library).
* `:durable` (boolean, optional): Specifies if the queues created for this consumer (main, retry, rejected) should be durable. Defaults to `true`.
* `:prefix` (String.t, optional): Overrides the global prefix (from `:queue_manager`) for queues created specifically for this consumer.
* Other options: Standard Broadway options like `:processors`, `:batchers`, `:prefetch_count`, `:name`, `:worker_count`, `:partitioned_by` are also supported and passed through to Broadway.

## Retry Mechanism Explained

When `:retry` is enabled for a consumer (by setting `retry: true` in `use Hutch.Broadway.RabbitProducer`):

1.  If `handle_message/3` returns an error or raises, `Hutch.RetryAcknowledger` (Hutch's internal mechanism) takes over.
2.  It checks message headers ("x-death") to count previous retry attempts.
3.  If `attempts < max_retries` (configured or default):
    * The message is rejected (`requeue: false`).
    * The main queue is configured by Hutch to dead-letter messages to a dedicated retry queue (e.g., `your_prefix.your_routing_key.retry`).
    * This retry queue has a message TTL (based on `:retry_interval`). After the TTL expires, RabbitMQ dead-letters the message back to the main processing queue for another attempt.
4.  If `attempts >= max_retries`:
    * The message is published by an internal instance of `Hutch.Publisher` to a final "rejected" queue (e.g., `your_prefix.your_routing_key.rejected`). `Hutch.RetryAcknowledger` ensures this publisher is available.
    * The original message is then ACKed from the main processing queue to remove it.

Messages in the "rejected" queue have a TTL (based on `:dlq_ttl`) before being discarded by RabbitMQ, allowing time for inspection or manual intervention if needed.

## License

This project is licensed under the [BSD 3-Clause License](LICENSE). ---
