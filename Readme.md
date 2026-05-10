# Transactional Outbox with Spring Boot, PostgreSQL, and Kafka

This project demonstrates the Transactional Outbox pattern using a minimal order workflow.

The order service stores business data and the corresponding integration event in the same database transaction. A scheduled outbox publisher later reads pending events and publishes them to Kafka.
## What this project demonstrates

- Transactional creation of an order and its corresponding outbox event
- PostgreSQL backed outbox table
- Scheduled outbox publisher
- Kafka publishing with producer acknowledgements
- Retry handling with retry count and next retry time
- DEAD state for events that exceed the retry limit
- Basic local observability through IntelliJ Database tools and Kafka UI
## Current architecture

The project currently contains an order service.

The order service exposes an HTTP endpoint for creating orders. When an order is created, the service stores:

1. The order aggregate
2. The order items
3. An `OrderCreated` event in the `outbox_events` table

These writes happen in the same local database transaction.
## Current event flow

```text
POST /orders
    |
    v
OrderApplicationService
    |
    |-- saves Order
    |-- saves OrderItems
    |-- saves OrderCreated event in outbox_events
    |
    v
OutboxPublisher
    |
    |-- reads publishable outbox events
    |-- sends event payload to Kafka
    |-- marks event as PUBLISHED after Kafka acknowledgement
    
```text
---

----

## Outbox states

این خیلی مهم است.

```md
## Outbox event states

| Status | Meaning |
|---|---|
| PENDING | Event has been created but not published yet |
| FAILED | Previous publish attempt failed, but the event can be retried |
| PUBLISHED | Kafka acknowledged the event |
| DEAD | Retry limit was reached and the event will not be retried automatically |
| PROCESSING | Event has been claimed by a publisher instance and is currently being published |

## Retry behavior

The publisher retries failed events using a simple linear backoff.

For example:

```text
retry_count = 1 -> retry after 10 seconds
retry_count = 2 -> retry after 20 seconds
retry_count = 3 -> retry after 30 seconds

---

## Guarantees

این بخش باید دقیق باشد.

```md
## Guarantees

The current implementation guarantees that an order and its corresponding outbox event are stored atomically in the order database.

It does not guarantee exactly once delivery.

The publisher provides at least once publishing semantics. If the service publishes an event to Kafka and crashes before marking the outbox row as `PUBLISHED`, the same event may be published again after restart.
Consumers must be idempotent.

## Current limitations

- The current publisher is intended for local single instance execution.
- It does not yet implement distributed row claiming for multiple service instances.
- It does not implement a Kafka consumer yet.
- It does not implement idempotent consumer handling yet.
- It does not implement a dead letter Kafka topic.
- It does not provide exactly once end to end processing.

## How to run

Start the local infrastructure:

```bash
docker compose up -d

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


---

## How to verify

```md
## How to verify

Check the `orders`, `order_items`, and `outbox_events` tables.

After order creation, the outbox event should first appear as `PENDING`.

After the scheduled publisher runs, the event should become `PUBLISHED`.

The published message can be inspected in Kafka UI under the `order.created.v1` topic.






1. status جدید PROCESSING اضافه کن
2. publisher اول event ها را claim کند
3. فقط event های claim شده را publish کند
4. اگر publish موفق شد -> PUBLISHED
5. اگر publish fail شد -> FAILED یا DEAD
6. اگر app بعد از claim کردن crash شد -> PROCESSING های قدیمی recover شوند

## Multi instance publishing

The outbox publisher claims events before publishing them. Claiming is done through PostgreSQL row locking using `FOR UPDATE SKIP LOCKED`.

This prevents two running instances of the order service from claiming and publishing the same outbox row at the same time.

Claimed events are marked as `PROCESSING`. After Kafka acknowledges the message, the event is marked as `PUBLISHED`. If publishing fails, the event is marked as `FAILED` or `DEAD` depending on the retry count.

A recovery job resets old `PROCESSING` events back to `FAILED`, so events do not remain stuck forever if the service crashes after claiming them.

This mechanism prevents concurrent publishing of the same row, but it does not provide exactly once delivery. Duplicate messages are still possible if the service crashes after Kafka acknowledges the message but before the outbox row is marked as `PUBLISHED`.

The publisher uses database row claiming to avoid multiple service instances publishing the same outbox row concurrently. This does not provide exactly once delivery. If the service crashes after Kafka acknowledges the message but before the row is marked as published, the event may be published again.


## Multi instance publishing

The outbox publisher claims events before publishing them. Claiming is done through PostgreSQL row locking using `FOR UPDATE SKIP LOCKED`.

This prevents two running instances of the order service from claiming and publishing the same outbox row at the same time.

Claimed events are marked as `PROCESSING`. After Kafka acknowledges the message, the event is marked as `PUBLISHED`. If publishing fails, the event is marked as `FAILED` or `DEAD` depending on the retry count.

A recovery job resets old `PROCESSING` events back to `FAILED`, so events do not remain stuck forever if the service crashes after claiming them.

This mechanism prevents concurrent publishing of the same row, but it does not provide exactly once delivery. Duplicate messages are still possible if the service crashes after Kafka acknowledges the message but before the outbox row is marked as `PUBLISHED`.


Transactional Outbox
+
Event Driven Workflow
+
یک Saga خیلی ساده


The outbox publisher claims rows using PostgreSQL FOR UPDATE SKIP LOCKED, marks them as PROCESSING, publishes them to Kafka, and updates the outbox status after Kafka acknowledgement. A recovery job resets stuck PROCESSING events for retry.
multi instance safe claiming
at least once publishing
not exactly once delivery
requires idempotent consumers


The publisher no longer reads and publishes events directly.
It first claims eligible rows using FOR UPDATE SKIP LOCKED, marks them as PROCESSING, and only then publishes them to Kafka.
A separate status update step marks events as PUBLISHED, FAILED, or DEAD.
A recovery job resets stale PROCESSING events so claimed events do not remain stuck forever.