# Kafka Order Stream

This service demonstrates Kafka as a durable, ordered, replayable event stream. The business example is order intake: an HTTP API accepts an order, appends an `orders.created` event to Kafka, and a consumer group builds a risk/customer-history projection from that stream.

## Architecture

```text
HTTP client
  |
  | POST /orders
  v
NestJS OrdersController
  |
  | validates payload and reads correlation ID
  v
OrdersService
  |
  | emits orders.created event keyed by customerId
  v
Kafka topic: orders.created
  |
  | consumed by group order-risk-projection
  v
OrderEventsConsumer
  |
  | calculates risk and logs projection decision
  v
Risk / customer timeline projection
```

| Component | File | Responsibility |
| --- | --- | --- |
| HTTP entrypoint | `src/orders.controller.ts` | Exposes `/orders` and `/health`. |
| Producer service | `src/orders.service.ts` | Builds `OrderCreatedEvent` and publishes to Kafka. |
| Consumer | `src/order-events.consumer.ts` | Consumes `orders.created` and calculates risk score. |
| Contract | `src/order-events.contract.ts` | Defines the event shape shared by producer and consumer. |
| Validation | `src/orders.dto.ts` | Validates incoming order payloads. |
| Broker config | `src/config.ts` | Reads Kafka brokers, topic, client IDs, and port. |
| Observability | `src/common/observability.ts` | Adds JSON logging, correlation IDs, HTTP logs, and exception logs. |

Kafka is used because order events are valuable historical facts. They should be retained, ordered by customer, and replayable by new consumers.

## Production Flow

In a production system, this flow would normally be split into separate deployable services:

| Production Unit | Role |
| --- | --- |
| `order-api` | Accepts user orders and appends immutable events to Kafka. |
| Kafka cluster | Stores events durably across partitions and replicas. |
| `risk-projection-worker` | Reads the event stream and flags suspicious orders. |
| `customer-history-worker` | Reads the same stream and builds customer timelines. |
| `analytics-worker` | Reads the same stream for reporting and metrics. |

Production request flow:

1. Client sends an order to the order API.
2. API validates the payload and creates an immutable event.
3. API publishes the event to Kafka with `customerId` as the message key.
4. Kafka stores the event in a partition. Events for the same `customerId` stay ordered.
5. API returns `201 Created` after the publish call completes.
6. Independent consumer groups process the event for risk, history, analytics, notifications, and audit.
7. If a new projection is needed later, a new consumer group can replay the topic from the beginning.

Production hardening usually adds:

| Concern | Production Approach |
| --- | --- |
| Schema safety | Use Schema Registry, Avro, Protobuf, or JSON Schema. |
| Duplicate handling | Add idempotency keys and idempotent consumers. |
| Failure isolation | Use retry topics and dead-letter topics. |
| Ordering | Key by `customerId`, `accountId`, or another domain aggregate ID. |
| Durability | Run a multi-broker Kafka cluster with replication. |
| Observability | Monitor consumer lag, partition skew, publish failures, and retry volume. |

## Functionality Flow

This is the exact local demo flow:

1. `POST /orders` receives JSON with `customerId`, `items`, and optional `couponCode`.
2. `HttpLoggingMiddleware` creates or forwards `x-correlation-id` and logs request start.
3. `ValidationPipe` rejects invalid payloads before business logic runs.
4. `OrdersController` reads optional `idempotency-key` so HTTP retries do not create duplicate events.
5. `OrdersService` returns a cached event when the idempotency key has already been processed.
6. `OrdersService` calculates the total, creates `orderId` and `eventId`, and builds an `OrderCreatedEvent` envelope with `eventType` and `schemaVersion`.
7. `OrdersService` publishes the event to Kafka topic `orders.created` with message key `customerId`.
8. HTTP response returns `accepted: true`, `duplicate`, the topic name, projection URL, and the event body.
9. `OrderEventsConsumer` receives the event from Kafka as part of consumer group `order-risk-projection`.
10. The consumer logs topic, partition, offset, order ID, total, customer ID, and risk score.
11. `OrderProjectionStore` updates the queryable customer read model.
12. If risk score is high, the consumer logs a manual review warning.
13. If risk score is normal, the consumer logs that the order was projected into the customer timeline.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Confirms the API is running and shows the Kafka topic. |
| `POST` | `/orders` | Accepts an order and appends an event to Kafka. |
| `GET` | `/customers/:customerId/projection` | Returns the customer order projection built by the Kafka consumer. |
| `GET` | `/projections` | Lists all in-memory customer projections. |

Create an order:

```bash
curl -X POST http://localhost:3001/orders \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-kafka-1' \
  -H 'idempotency-key: order-demo-1' \
  -d '{"customerId":"customer-42","items":[{"sku":"camera","quantity":1,"unitPrice":1299.99},{"sku":"memory-card","quantity":2,"unitPrice":39.5}],"couponCode":"VIP"}'
```

Example response:

```json
{
  "accepted": true,
  "duplicate": false,
  "reason": "Order was appended to the Kafka stream for downstream projections.",
  "topic": "orders.created",
  "projectionUrl": "/customers/customer-42/projection",
  "event": {
    "eventType": "order.created",
    "schemaVersion": 1,
    "eventId": "uuid",
    "orderId": "uuid",
    "customerId": "customer-42",
    "total": 1378.99,
    "couponCode": "VIP",
    "correlationId": "demo-kafka-1"
  }
}
```

Query the projection after the consumer handles the event:

```bash
curl http://localhost:3001/customers/customer-42/projection
```

## Run

Run only this service and Kafka:

```bash
cd /home/hasnain/Desktop/Projects/nestjs-broker-problem-suite/kafka-order-stream
docker compose up --build
```

Run the full suite from the root folder:

```bash
cd /home/hasnain/Desktop/Projects/nestjs-broker-problem-suite
docker compose up --build
```

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `3001` | HTTP port. |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list. |
| `KAFKA_ORDER_TOPIC` | `orders.created` | Topic used for order events. |
| `KAFKA_CLIENT_ID` | `order-api` | Producer client ID. |
| `KAFKA_CONSUMER_CLIENT_ID` | `order-risk-projection` | Consumer client ID. |
| `KAFKA_CONSUMER_GROUP` | `order-risk-projection` | Consumer group ID. |

## Logging

Important log events:

| Log Message | Meaning |
| --- | --- |
| `HTTP request started` | Request reached the API. |
| `Duplicate order request resolved from idempotency cache` | Same idempotency key was safely replayed. |
| `Publishing order event to Kafka` | Producer is sending an order event. |
| `Order event committed to Kafka producer buffer` | Publish call completed from the API perspective. |
| `Kafka order event consumed for projection` | Consumer received the event and has Kafka metadata. |
| `Customer projection updated from Kafka stream` | Read model was updated from the consumed event. |
| `High-risk order flagged for manual review` | Risk score crossed the local demo threshold. |
| `Order projected into customer timeline` | Normal projection flow completed. |
