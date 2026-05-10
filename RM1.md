# Transactional Outbox Kafka Demo

This project demonstrates the Transactional Outbox pattern using Spring Boot, PostgreSQL, and Kafka.

The current implementation focuses on the order service. When an order is created, the service stores the order data and the corresponding integration event in the same PostgreSQL transaction. A scheduled outbox publisher later reads publishable events from the outbox table and publishes them to Kafka.

This project is intentionally small. The goal is to make the reliability problem visible, not to build a full ecommerce system.

## Current scope

The current implementation includes:

- Order creation through an HTTP endpoint
- Order items stored with the order
- `OrderCreated` event creation
- PostgreSQL backed `outbox_events` table
- Scheduled outbox publisher
- Kafka publishing with producer acknowledgement
- Retry handling with retry count and next retry time
- `DEAD` state for events that exceed the retry limit
- Local verification through IntelliJ HTTP Client, IntelliJ Database tools, and Kafka UI

The inventory service and idempotent consumers are planned for the next stage.

## Architecture

```text
POST /orders
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
PostgreSQL transaction commits
    |
    v
OutboxPublisher
    |
    |-- reads publishable events
    |-- sends event payload to Kafka
    |-- waits for Kafka acknowledgement
    |-- marks event as PUBLISHED, FAILED, or DEAD
```

## Package structure

The order service uses a clean architecture inspired structure:

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

The project is clean architecture inspired, not a strict Clean Architecture implementation. The domain model currently uses JPA annotations for simplicity.

## Transactional Outbox flow

When a client creates an order, the service writes both the business data and the event data in one local database transaction.

```text
orders
order_items
outbox_events
```

This guarantees that if the order is committed, the corresponding `OrderCreated` outbox event is also committed.

The service does not publish to Kafka inside the order creation transaction. Kafka publishing is handled later by the scheduled outbox publisher.

## Outbox event states

| Status | Meaning |
|---|---|
| `PENDING` | The event has been created but not published yet. |
| `FAILED` | A previous publish attempt failed, but the event can be retried. |
| `PUBLISHED` | Kafka acknowledged the event. |
| `DEAD` | The retry limit was reached and the event will not be retried automatically. |

## Retry behavior

The publisher retries failed events using a simple linear backoff.

```text
retry_count = 1  -> retry after 10 seconds
retry_count = 2  -> retry after 20 seconds
retry_count = 3  -> retry after 30 seconds
...
```

When the retry count reaches the configured maximum, the event is marked as `DEAD`.

Example configuration:

```yaml
app:
  outbox:
    max-retries: 10
    fixed-delay-ms: 3000
```

## Kafka publishing behavior

The publisher sends the event payload to Kafka and waits for acknowledgement:

```text
KafkaTemplate.send(...).get(5, TimeUnit.SECONDS)
```

This makes the publishing attempt synchronous from the publisher thread's point of view.

If Kafka acknowledges the message, the outbox row is marked as `PUBLISHED`.

If Kafka is unavailable, slow, or the send operation fails, the event is marked as `FAILED` or `DEAD` depending on the retry count.

## Guarantees

The current implementation guarantees:

- Order data and the `OrderCreated` outbox event are stored atomically in PostgreSQL.
- An event is not lost just because Kafka is temporarily unavailable.
- Failed publish attempts remain visible in the database.
- Failed events can be retried later.

The current implementation does not guarantee:

- Exactly once delivery
- Exactly once processing
- End to end business completion
- Consumer side processing
- Multi instance safe publishing

The publisher provides at least once publishing semantics. If Kafka acknowledges a message and the service crashes before the outbox row is marked as `PUBLISHED`, the same event may be published again later.

Consumers must be idempotent.

## Multi instance limitation

The current publisher is suitable for local single instance execution.

If two instances of the order service run at the same time, both instances may read the same publishable outbox row and publish the same event.

A production oriented version should add row claiming, for example:

- Add a `PROCESSING` status
- Claim rows in a short transaction
- Use PostgreSQL `FOR UPDATE SKIP LOCKED`
- Mark claimed rows as `PROCESSING`
- Publish to Kafka outside the claim transaction
- Mark rows as `PUBLISHED`, `FAILED`, or `DEAD`
- Recover old stuck `PROCESSING` rows

This prevents two service instances from publishing the same outbox row concurrently. It still does not provide exactly once delivery, because a crash after Kafka acknowledgement and before database update can still cause duplicate publishing.

## Local infrastructure

The project uses Docker Compose for local infrastructure.

Expected services:

```text
order-db
inventory-db
kafka
kafka-ui
```

Start infrastructure:

```bash
docker compose up -d
```

Stop infrastructure:

```bash
docker compose down
```

Reset local volumes:

```bash
docker compose down -v
docker compose up -d
```

Use volume reset only for local development because it deletes local PostgreSQL data.

## Creating an order

Use IntelliJ HTTP Client with a file such as:

```text
http/order-service.http
```

Example request:

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

Expected successful response:

```json
{
  "orderId": "..."
}
```

## Validation examples

Empty item list should return `400 Bad Request`:

```http
POST http://localhost:8081/orders
Content-Type: application/json

{
  "items": []
}
```

Missing quantity should return `400 Bad Request` if nested validation is enabled:

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

## Database verification

After creating an order, verify the database tables:

```sql
SELECT * FROM orders;

SELECT * FROM order_items;

SELECT id, aggregate_type, aggregate_id, event_type, status, created_at, published_at, retry_count, next_retry_at, last_error
FROM outbox_events
ORDER BY created_at DESC;
```

Expected result after order creation:

```text
orders: one new row
order_items: one or more new rows
outbox_events: one new row with event_type = OrderCreated
```

Before publishing, the outbox row should be `PENDING`.

After the scheduled publisher runs successfully, the outbox row should become `PUBLISHED`.

## Kafka UI verification

Kafka UI can be used to verify that the event reached Kafka.

Check:

- Topic name: `order.created.v1`
- Message key: order aggregate ID
- Message payload: `OrderCreated` event JSON
- Message timestamp
- Offset

Expected event payload shape:

```json
{
  "eventId": "...",
  "orderId": "...",
  "items": [
    {
      "skuId": "...",
      "quantity": 2
    }
  ]
}
```

Kafka UI confirms that the event was published to Kafka. It does not confirm that a downstream service processed the event.

## Failure test

To test retry behavior:

1. Create an order while Kafka is running.
2. Stop Kafka.
3. Create another order or wait for pending events to be retried.
4. Observe `FAILED` status and increasing `retry_count`.
5. Start Kafka again.
6. Observe that retryable events eventually become `PUBLISHED`.
7. Events that exceed `max-retries` become `DEAD`.

A `DEAD` event is not automatically retried by the current implementation.

## Current limitations

- No inventory consumer yet
- No idempotent consumer yet
- No dead letter Kafka topic yet
- No multi instance row claiming yet
- No `PROCESSING` state yet
- No recovery job for stuck processing events yet
- No distributed tracing
- No production Kafka configuration
- No exactly once end to end processing

## Planned next steps

- Add `PROCESSING` status and row claiming with `FOR UPDATE SKIP LOCKED`
- Add recovery for stuck `PROCESSING` events
- Add inventory service consumer for `OrderCreated`
- Add idempotency table for consumed events
- Add inventory result events:
    - `InventoryReserved`
    - `InventoryReservationFailed`
- Add order service consumer for inventory result events
- Update order status based on inventory outcome
