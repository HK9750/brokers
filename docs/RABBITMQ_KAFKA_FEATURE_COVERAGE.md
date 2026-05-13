# RabbitMQ And Kafka Feature Coverage

This workspace gives you two practical NestJS backends:

| Project | Broker | Unique Problem | Local API |
| --- | --- | --- | --- |
| `rabbitmq-report-jobs` | RabbitMQ | Slow report generation as durable background work. | `http://localhost:3002` |
| `kafka-order-stream` | Kafka | Durable order event stream with replayable customer projections. | `http://localhost:3001` |

The goal is not to pretend RabbitMQ and Kafka are interchangeable. RabbitMQ is a message router and work queue. Kafka is a durable event log.

## Run Only RabbitMQ And Kafka Projects

From the suite root:

```bash
cd /home/hasnain/Desktop/Projects/nestjs-broker-problem-suite
docker compose -f docker-compose.rabbitmq-kafka.yml up --build
```

Health checks:

```bash
curl http://localhost:3001/health
curl http://localhost:3002/health
```

Broker tools:

| Tool | URL |
| --- | --- |
| RabbitMQ Management UI | `http://localhost:15672`, username `guest`, password `guest` |
| Kafka external listener | `localhost:29092` |

## RabbitMQ: What It Offers

RabbitMQ is best when you need broker-controlled delivery of work to consumers.

| Capability | What It Means | Covered In This Project |
| --- | --- | --- |
| Producers | Code publishes messages without doing the slow work inline. | `ReportsService.enqueueReport` publishes report jobs. |
| Consumers | Workers pull messages from queues. | `ReportsWorker.handleReportJob` consumes jobs. |
| Queues | Durable mailboxes for messages waiting to be processed. | `report.jobs` queue. |
| Durable queues | Queue survives broker restart. | `queueOptions: { durable: true }`. |
| Persistent messages | Message should survive broker restart when published persistently. | Nest RMQ transport is used; for explicit publisher confirms/persistence use raw `amqplib`. |
| Manual ack | Consumer confirms successful processing. | Worker calls `channel.ack(message)`. |
| Nack/reject | Consumer rejects failed processing. | Worker calls `channel.nack(message, false, false)`. |
| Redelivery flag | Broker marks messages that were previously delivered. | Worker logs `message.fields.redelivered`. |
| Prefetch | Limits unacked messages per worker for back-pressure. | `prefetchCount: 1` in `main.ts`. |
| Competing consumers | Multiple workers share one queue. | Scale `rabbitmq-report-api` replicas in Docker or production. |
| Exchanges | Router that receives published messages. | Nest RMQ hides most exchange setup; RabbitMQ concepts are documented here. |
| Direct exchange | Routes by exact routing key. | Common for named task routing. |
| Topic exchange | Routes by wildcard routing keys like `reports.*`. | Useful for event-style routing. |
| Fanout exchange | Broadcasts to all bound queues. | Useful for notifications to many worker types. |
| Headers exchange | Routes using message headers. | Useful when routing keys are not enough. |
| Bindings | Connect exchanges to queues with routing rules. | Use raw `amqplib` or broker definitions for advanced demos. |
| Dead-letter exchange | Failed or expired messages go to a failure queue. | Discussed in README; recommended production addition. |
| TTL | Message or queue expiration. | Production addition for expiring stale work. |
| Priority queues | Higher-priority messages are delivered first. | DTO includes priority; RabbitMQ priority queue config is a production addition. |
| Delayed retries | Failed messages wait before retrying. | Usually implemented with TTL plus DLX or delayed-message plugin. |
| RPC pattern | Request waits for a response through reply queue/correlation ID. | Not ideal for long reports; this project intentionally uses async job status. |
| Publisher confirms | Broker confirms that a publish reached the broker. | Use raw `amqplib` confirm channels for strict guarantees. |
| Quorum queues | Replicated durable queue type for high availability. | Production cluster option. |
| Stream queues | RabbitMQ stream feature for log-like workloads. | If you need this heavily, Kafka is usually the cleaner fit. |
| Management UI | Browser UI for queues, exchanges, consumers, rates, and messages. | Exposed at `:15672`. |
| Policies | Broker-side rules for DLX, TTL, HA, limits. | Production operation concern. |
| Federation/Shovel | Move messages between brokers or regions. | Production integration concern. |

RabbitMQ demo requests:

```bash
curl -X POST http://localhost:3002/reports \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-rabbit-1' \
  -d '{"type":"sales","requestedBy":"finance@example.com","dateFrom":"2026-01-01","dateTo":"2026-01-31","priority":"high"}'
```

```bash
curl -X POST http://localhost:3002/reports \
  -H 'content-type: application/json' \
  -d '{"type":"compliance","requestedBy":"audit@example.com","dateFrom":"2026-01-01","dateTo":"2026-01-31","priority":"critical","simulateFailure":true}'
```

## Kafka: What It Offers

Kafka is best when the messages are historical facts that many consumers may need to read independently.

| Capability | What It Means | Covered In This Project |
| --- | --- | --- |
| Producers | Code appends records to a topic. | `OrdersService.createOrder` emits `orders.created`. |
| Topics | Named append-only streams. | `orders.created`. |
| Partitions | Ordered shards of a topic. | Topic is created with 3 partitions. |
| Message keys | Key chooses the partition and preserves per-key order. | `customerId` is used as message key. |
| Offsets | Position of a record in a partition. | Consumer logs offset metadata. |
| Consumer groups | Group members share partitions. | `order-risk-projection` group. |
| Independent consumers | Different groups can read the same events separately. | Add another consumer group for analytics or notifications. |
| Retention | Kafka keeps messages after consumption. | Configurable broker/topic setting. |
| Replay | Reset offsets or use a new group to rebuild state. | Projection model demonstrates why replay matters. |
| Ordering | Guaranteed inside one partition, not globally. | Same `customerId` stays ordered. |
| Headers | Metadata alongside message value. | Correlation ID is sent in headers. |
| Event envelope | Standard event metadata around domain data. | `eventType`, `schemaVersion`, `eventId`, `correlationId`. |
| Idempotency | Duplicate requests/events should be safe. | HTTP idempotency key cache and projection by event ID. |
| Batching | Producer can batch records for throughput. | KafkaJS/Nest defaults can be tuned in production. |
| Compression | Producer can compress batches. | Production producer option. |
| Acks | Producer waits for broker acknowledgements. | Production producer option, usually `acks=all`. |
| Idempotent producer | Broker prevents duplicate writes during retries. | Production producer option. |
| Transactions | Atomic writes across partitions/topics and offset commits. | Production option for exactly-once stream processing. |
| Log compaction | Retain latest record per key. | Useful for state topics; not needed for immutable order events. |
| Tombstones | Null-value records delete compacted keys. | Used with compacted topics. |
| Retry topics | Failed events move through retry streams. | Production addition. |
| Dead-letter topics | Poison events are isolated for inspection. | Production addition. |
| Schema registry | Enforces event compatibility. | Recommended with Avro, Protobuf, or JSON Schema. |
| Consumer lag | Difference between latest offset and consumed offset. | Production metric to monitor. |
| Rebalancing | Partitions move between group members as workers change. | Happens automatically for consumer groups. |
| Replication | Partitions are copied across brokers. | Single-node local Docker uses replication factor `1`; production uses `3+`. |
| KRaft | Kafka's built-in metadata quorum, replacing ZooKeeper. | Local Docker uses KRaft mode. |
| ACLs/SASL/TLS | Authentication, authorization, encryption. | Production security concern. |
| Kafka Streams/ksqlDB | Stream processing and derived tables. | This project uses a simple NestJS consumer instead. |

Kafka demo request:

```bash
curl -X POST http://localhost:3001/orders \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-kafka-1' \
  -H 'idempotency-key: order-demo-1' \
  -d '{"customerId":"customer-42","items":[{"sku":"camera","quantity":1,"unitPrice":1299.99},{"sku":"memory-card","quantity":2,"unitPrice":39.5}],"couponCode":"VIP"}'
```

Then query the projection:

```bash
curl http://localhost:3001/customers/customer-42/projection
```

## What To Notice While Running

| Question | RabbitMQ Answer | Kafka Answer |
| --- | --- | --- |
| What happens after consumption? | Message leaves the queue after ack. | Event remains in the topic until retention removes it. |
| Who tracks progress? | Broker tracks unacked queue deliveries. | Consumer group tracks offsets. |
| Can another service read the same data? | Usually needs another queue binding. | Yes, create another consumer group. |
| Can I rebuild a projection later? | Not naturally; messages are consumed and gone. | Yes, replay from stored offsets/history. |
| How do I control worker load? | Prefetch and queue depth. | Partitions, consumer count, and lag. |
| What is the strongest fit? | Jobs, commands, retries, task routing. | Event history, analytics, projections, audit. |

## Learning Order

1. Start `docker-compose.rabbitmq-kafka.yml`.
2. Send one RabbitMQ report request and watch `docker compose logs -f rabbitmq-report-api`.
3. Send one failing RabbitMQ report request and inspect the Management UI.
4. Send one Kafka order request and watch `docker compose logs -f kafka-order-api`.
5. Send the same Kafka request with the same `idempotency-key` and compare the response.
6. Query the Kafka projection endpoint to see broker data turned into queryable state.
7. Read each service README, then read `docs/BROKER_INTERNALS_AND_PRODUCTION_GUIDE.md` for deeper production details.
