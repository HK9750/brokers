# NestJS Broker Problem Suite

This repository contains four independent NestJS backends. Each backend demonstrates a broker doing the job it is actually good at, instead of using every broker as a generic queue.

For broker internals, production architecture, and easy engineering explanations, read `docs/BROKER_INTERNALS_AND_PRODUCTION_GUIDE.md`.

The suite is designed as a practical comparison:

| Service | Broker | Main Problem | HTTP Port |
| --- | --- | --- | --- |
| `kafka-order-stream` | Kafka | Durable ordered event stream for order timelines, analytics, and replayable projections. | `3001` |
| `rabbitmq-report-jobs` | RabbitMQ | Durable background work queue with manual acknowledgement and worker back-pressure. | `3002` |
| `nats-device-control` | NATS | Low-latency request-reply commands and lightweight telemetry fanout. | `3003` |
| `redis-presence-pubsub` | Redis Pub/Sub | Ephemeral presence fanout where missed events are acceptable. | `3004` |

## Start Everything

Run all APIs and all brokers from the suite root:

```bash
cd /home/hasnain/Desktop/Projects/nestjs-broker-problem-suite
docker compose up --build
```

Equivalent absolute command:

```bash
docker compose -f /home/hasnain/Desktop/Projects/nestjs-broker-problem-suite/docker-compose.yml up --build
```

Stop everything:

```bash
docker compose down
```

Stop everything and remove Redis, RabbitMQ, and Postgres volumes:

```bash
docker compose down -v
```

## Suite Architecture

```text
Clients / Postman
  |
  | HTTP
  |
  +-- :3001 kafka-order-api ---- Kafka topic: orders.created
  |
  +-- :3002 rabbitmq-report-api - RabbitMQ queue: report.jobs
  |
  +-- :3003 nats-device-api ----- NATS subjects: device.command, device.telemetry
  |
  +-- :3004 redis-presence-api -- Redis Pub/Sub channel: presence.changed

All four APIs also use Postgres for durable state:

  +-- postgres:5432 ---------- durable projections, job statuses, device state, presence state
```

Each service is intentionally self-contained for local learning:

| Layer | Responsibility |
| --- | --- |
| NestJS HTTP API | Accepts validated JSON requests and returns API responses. |
| Broker producer | Publishes events, jobs, commands, or pub/sub messages. |
| Broker consumer | Demonstrates downstream processing and broker-specific behavior. |
| Postgres state store | Persists read models and operational state that should survive restarts. |
| Structured logger | Emits JSON logs with correlation IDs, request metadata, broker metadata, and business decisions. |
| Docker Compose | Runs the API and the required broker with practical health checks. |

The code now also includes production-shaped read/state flows:

| Service | Production-Style Functionality |
| --- | --- |
| Kafka | Idempotency key handling and Postgres-backed customer projections built from consumed events. |
| RabbitMQ | Postgres-backed report job status store with queued, processing, completed, and failed states. |
| NATS | Postgres-backed device heartbeat registry that gates command request-reply behavior. |
| Redis Pub/Sub | Postgres-backed current presence state and room presence lookup with TTL-style expiry. |

In production, the producer and consumer parts would normally be deployed as separate processes. They are combined here so one Docker Compose command can demonstrate the full flow.

## Production Flow Comparison

| Broker | Production Flow | Best Fit | Not Ideal For |
| --- | --- | --- | --- |
| Kafka | APIs append immutable events to topics. Multiple consumer groups build independent projections. Events can be replayed. | Event sourcing, analytics streams, audit trails, ordered per-key timelines. | Simple one-off jobs where a worker just needs to process and delete work. |
| RabbitMQ | APIs publish durable jobs. Worker pools consume with prefetch, acknowledge success, and dead-letter failures. | Background jobs, task queues, email/report generation, workload smoothing. | Long-term event history and replayable analytics. |
| NATS | Services communicate through lightweight subjects. Request-reply handles fast commands, and fanout handles live telemetry. | Low-latency commands, internal service mesh, live control-plane messaging. | Durable audit history unless JetStream is enabled. |
| Redis Pub/Sub | Publishers broadcast ephemeral messages to active subscribers. Offline subscribers miss messages. | Presence, typing indicators, online/offline fanout, live UI hints. | Payments, orders, reports, or anything requiring durability. |

## Health Checks

After `docker compose up --build`, test all APIs:

```bash
curl http://localhost:3001/health
curl http://localhost:3002/health
curl http://localhost:3003/health
curl http://localhost:3004/health
```

## Postman

Import these two files into Postman:

| File | Purpose |
| --- | --- |
| `postman/nestjs-broker-problem-suite.postman_collection.json` | Requests and tests for all APIs. |
| `postman/local.postman_environment.json` | Localhost base URL variables. |

Recommended Postman order:

1. Import the environment file.
2. Import the collection file.
3. Select `NestJS Broker Suite Local` as the active environment.
4. Run the `Health` request in each folder.
5. Run the functional requests and watch Docker logs.

## Documentation Map

Each service README explains architecture, production flow, and functionality flow in detail:

| File | Documentation Focus |
| --- | --- |
| `kafka-order-stream/README.md` | Kafka order event stream and projection flow. |
| `rabbitmq-report-jobs/README.md` | RabbitMQ durable job queue and worker acknowledgement flow. |
| `nats-device-control/README.md` | NATS command request-reply and telemetry fanout flow. |
| `redis-presence-pubsub/README.md` | Redis Pub/Sub ephemeral presence fanout flow. |
| `docs/BROKER_INTERNALS_AND_PRODUCTION_GUIDE.md` | Broker internals, production diagrams, tradeoffs, and failure modes. |

## Logging

All services produce structured JSON logs. Logs include:

| Field Type | Examples |
| --- | --- |
| Request metadata | `correlationId`, `method`, `path`, `statusCode`, `durationMs`. |
| Broker metadata | topic, queue, subject, channel, partition, offset, delivery tag. |
| Business metadata | `orderId`, `jobId`, `commandId`, `eventId`, `userId`, `riskScore`. |
| Operational decisions | ack, nack, threshold warning, high-risk flag, offline cleanup. |

View logs for one service:

```bash
docker compose logs -f kafka-order-api
docker compose logs -f rabbitmq-report-api
docker compose logs -f nats-device-api
docker compose logs -f redis-presence-api
```

## Broker UIs And Tools

| Broker | Local Access |
| --- | --- |
| Kafka | Broker is exposed on `localhost:29092`. |
| RabbitMQ | Management UI at `http://localhost:15672`, login `guest` / `guest`. |
| NATS | Monitoring endpoint at `http://localhost:8222`. |
| Redis | Redis is exposed on `localhost:6379`. |
| Postgres | Database is exposed on `localhost:5432`, user `broker_suite`, password `broker_suite`, database `broker_suite`. |

## Project Layout

```text
nestjs-broker-problem-suite/
  docker-compose.yml
  docs/
  postman/
  kafka-order-stream/
  rabbitmq-report-jobs/
  nats-device-control/
  redis-presence-pubsub/
```

Each subfolder has its own `package.json`, `Dockerfile`, `docker-compose.yml`, `.env.example`, NestJS source files, and service-specific README.
