# Transactional Outbox Kafka Demo

This project demonstrates the Transactional Outbox pattern with Spring Boot, PostgreSQL, and Kafka.

The current implementation focuses on the `order-service`. When an order is created, the service stores the order, its order items, and the corresponding `OrderCreated` event in the same local database transaction. A scheduled publisher later reads publishable outbox events and sends them to Kafka.

The project is intentionally small. It is designed to show the reliability boundary of the outbox pattern, not to act as a production ready microservices platform.

## Current scope

The current version includes:

- Order creation through an HTTP endpoint
- Order items stored with the order
- An `outbox_events` table
- Atomic persistence of business data and the integration event
- Scheduled outbox publishing to Kafka
- Kafka producer acknowledgement handling
- Retry handling with retry count and next retry time
- A `DEAD` state for events that exceed the retry limit
- Local inspection through IntelliJ HTTP Client, IntelliJ Database tools, and Kafka UI

The current version does not yet include the inventory service or downstream event processing.

## Architecture overview

```text
HTTP Client
    |
    v
OrderController
    |
    v
OrderApplicationService
    |
    |-- saves Order
    |-- saves OrderItems
    |-- saves OrderCreated event in outbox_events
    |
    v
PostgreSQL
    |
    v
OutboxPublisher
    |
    |-- reads publishable outbox events
    |-- publishes event payload to Kafka
    |-- marks event as PUBLISHED after Kafka acknowledgement
```

## Package structure

The order service uses a clean architecture inspired package structure.

```text
com.shirin.order
├── api
│   ├── OrderController
│   ├── CreateOrderRequest
│   ├── OrderItemRequest
│   └── CreateOrderResponse
│
├── application
│   ├── OrderApplicationService
│   ├── CreateOrderCommand
│   └── CreateOrderCommandItem
│
├── domain
│   ├── Order
│   ├── OrderItem
│   ├── OrderStatus
│   └── OrderRepository
│
├── messaging.events
│   ├── OrderCreatedEvent
│   └── OrderCreatedEventItem
│
└── outbox
    ├── OutboxEvent
    ├── OutboxStatus
    ├── OutboxEventRepository
    └── OutboxPublisher
```

The package layout separates HTTP API models, application use cases, domain objects, messaging contracts, and outbox persistence.

This is not a strict Clean Architecture implementation because the domain model still uses JPA annotations. The goal is to keep the project simple while preserving clear boundaries between the main concerns.

## Transactional Outbox flow

When a client creates an order, the service performs the following operations inside one local database transaction:

```text
1. Create Order
2. Create OrderItems
3. Create OrderCreated event
4. Store the event in outbox_events with status PENDING
```

This means that the order and the integration event are committed together.

The outbox publisher then runs on a schedule:

```text
1. Read publishable outbox events
2. Publish each event to Kafka
3. Wait for Kafka acknowledgement
4. Mark the event as PUBLISHED
5. If publishing fails, mark the event as FAILED or DEAD
```

## Outbox event states

| Status | Meaning |
|---|---|
| `PENDING` | The event was created but has not been published yet. |
| `FAILED` | A previous publish attempt failed, but the event can still be retried. |
| `PUBLISHED` | Kafka acknowledged the event. |
| `DEAD` | The retry limit was reached and the event will not be retried automatically. |

## Retry behaviour

The publisher uses a simple linear backoff.

```text
retry_count = 1 -> retry after 10 seconds
retry_count = 2 -> retry after 20 seconds
retry_count = 3 -> retry after 30 seconds
```

The next retry time is calculated in the domain model:

```java
this.nextRetryAt = Instant.now().plusSeconds(10L * this.retryCount);
```

When `retry_count` reaches the configured maximum, the event is marked as `DEAD`.

## Kafka publishing behaviour

The publisher sends the event payload to Kafka and waits for acknowledgement:

```java
kafkaTemplate
        .send(orderCreatedTopic, event.getAggregateId().toString(), event.getPayload())
        .get(5, TimeUnit.SECONDS);
```

Although `KafkaTemplate.send(...)` is asynchronous, calling `.get(...)` makes the current publisher thread wait until Kafka acknowledges the message or the timeout is reached.

The publisher processes the selected events sequentially. If Kafka is unavailable, each event may wait until the timeout before the publisher moves to the next event.

## Guarantees

The current implementation guarantees that an order and its corresponding outbox event are stored atomically in the order service database.

It does not guarantee exactly once delivery.

The publisher provides at least once publishing semantics. If the service publishes an event to Kafka and crashes before marking the outbox row as `PUBLISHED`, the same event may be published again after restart.

Future consumers must be idempotent.

## Current limitations

The current implementation is suitable for a local single instance demo.

It does not yet implement:

- Distributed row claiming for multiple running instances of `order-service`
- `PROCESSING` state for claimed outbox rows
- `SELECT ... FOR UPDATE SKIP LOCKED`
- Recovery of stuck `PROCESSING` rows
- Inventory service
- Idempotent consumer handling
- Retry topics
- Kafka dead letter topic
- End to end saga flow
- Exactly once end to end processing

## Multi instance limitation

The current publisher reads publishable rows and publishes them sequentially. This is fine for local development with one running instance.

If two instances of `order-service` run at the same time, both instances may read the same `PENDING` event before either one updates its status.

Example:

```text
event 123 = PENDING

Instance A reads event 123
Instance B reads event 123

Instance A publishes event 123 to Kafka
Instance B also publishes event 123 to Kafka
```

This can create duplicate messages.

A production oriented version should claim rows before publishing them. One common approach is:

```text
1. Add PROCESSING status
2. Select publishable rows using SELECT ... FOR UPDATE SKIP LOCKED
3. Mark selected rows as PROCESSING in a short database transaction
4. Publish claimed rows to Kafka outside the claim transaction
5. Mark rows as PUBLISHED, FAILED, or DEAD in a second short transaction
6. Reset old PROCESSING rows back to FAILED if the service crashes after claiming them
```

This prevents multiple instances from publishing the same outbox row at the same time. It still does not provide exactly once delivery, because the service can still crash after Kafka acknowledgement and before the database row is marked as `PUBLISHED`.

## How to run

Start the local infrastructure:

```bash
docker compose up -d
```

Run `OrderServiceApplication` from IntelliJ.

Create an order using IntelliJ HTTP Client.

```http
POST http://localhost:8081/orders
Content-Type: application/json

{
  "items": [
    {
      "skuId": "11111111-1111-1111-1111-111111111111",
      "quantity": 2
    }
  ]
}
```

A successful response should contain the created order id.

```json
{
  "orderId": "..."
}
```

## Validation examples

An empty item list should return `400 Bad Request`.

```http
POST http://localhost:8081/orders
Content-Type: application/json

{
  "items": []
}
```

An item without quantity should also return `400 Bad Request`.

```http
POST http://localhost:8081/orders
Content-Type: application/json

{
  "items": [
    {
      "skuId": "11111111-1111-1111-1111-111111111111",
      "quantity": 2
    },
    {
      "skuId": "11111111-1111-1111-2222-111111111111"
    }
  ]
}
```

The request model should validate nested items. The item list should be declared with `@Valid`, and `quantity` should use `Integer` with `@NotNull` and `@Positive`.

## How to verify in the database

Use IntelliJ Ultimate Database tools or another PostgreSQL client.

Check the order tables:

```sql
SELECT * FROM orders;
SELECT * FROM order_items;
```

Check the outbox table:

```sql
SELECT id,
       aggregate_type,
       aggregate_id,
       event_type,
       status,
       retry_count,
       next_retry_at,
       created_at,
       published_at,
       last_error
FROM outbox_events
ORDER BY created_at DESC;
```

After order creation, the event should first appear as `PENDING`.

After the scheduled publisher runs and Kafka acknowledges the message, the event should become `PUBLISHED`.

If Kafka is unavailable, the event should become `FAILED`, its `retry_count` should increase, and `next_retry_at` should be set. After the retry limit is reached, the event should become `DEAD`.

## How to verify in Kafka UI

Open Kafka UI and inspect the configured topic:

```text
order.created.v1
```

The topic should contain messages with payloads similar to:

```json
{
  "eventId": "...",
  "orderId": "...",
  "items": [
    {
      "skuId": "11111111-1111-1111-1111-111111111111",
      "quantity": 2
    }
  ]
}
```

The message key is the aggregate id, which is the order id. This helps keep messages for the same order on the same Kafka partition when multiple partitions are used.

## Suggested next steps

The next implementation steps are:

1. Add `PROCESSING` status and row claiming for safer multi instance publishing
2. Add `SELECT ... FOR UPDATE SKIP LOCKED`
3. Add recovery for stuck `PROCESSING` rows
4. Add `inventory-service`
5. Consume `OrderCreated` in `inventory-service`
6. Add idempotent consumer handling
7. Publish `InventoryReserved` or `InventoryReservationFailed`
8. Consume inventory result events in `order-service`
9. Update order status based on the inventory result
