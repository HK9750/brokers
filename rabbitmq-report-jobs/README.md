# RabbitMQ Report Jobs

This service demonstrates RabbitMQ as a durable background job queue. The business example is report generation: an HTTP API accepts a report request, enqueues a durable job, and a worker processes that job with explicit acknowledgement.

## Architecture

```text
HTTP client
  |
  | POST /reports
  v
NestJS ReportsController
  |
  | validates request and returns quickly
  v
ReportsService
  |
  | emits report.generate message
  v
RabbitMQ queue: report.jobs
  |
  | worker consumes with prefetchCount=1 and noAck=false
  v
ReportsWorker
  |
  | generates artifact and manually acks
  v
Report artifact location log
```

| Component | File | Responsibility |
| --- | --- | --- |
| HTTP entrypoint | `src/reports.controller.ts` | Exposes `/reports` and `/health`. |
| Producer service | `src/reports.service.ts` | Builds `ReportJob` and publishes it to RabbitMQ. |
| Worker | `src/reports.worker.ts` | Consumes jobs, simulates report generation, and manually acks or nacks. |
| Contract | `src/report.contract.ts` | Defines the job payload shared by producer and worker. |
| Validation | `src/reports.dto.ts` | Validates report type, date range, requester, and priority. |
| Broker config | `src/config.ts` | Reads RabbitMQ URL, queue name, pattern, port, and simulated work time. |
| Database | `src/common/postgres.service.ts` | Owns the Postgres connection pool used by the job status store. |
| Observability | `src/common/observability.ts` | Adds JSON logging, correlation IDs, HTTP logs, and exception logs. |

RabbitMQ is used because report generation is slow work. The API should not keep the HTTP client waiting while a worker creates a file.

## Production Flow

In a production system, this would normally be deployed as separate producer and worker services:

| Production Unit | Role |
| --- | --- |
| `report-api` | Accepts report requests and publishes durable jobs. |
| RabbitMQ cluster | Buffers jobs and handles delivery to workers. |
| `report-worker` pool | Consumes jobs, generates reports, and acknowledges completion. |
| Storage service | Stores generated PDF/CSV/XLSX artifacts. |
| Notification service | Notifies users when reports are ready. |

Production request flow:

1. Client requests a report.
2. API validates the request and publishes a durable job to RabbitMQ.
3. API immediately returns `201 Created` with a `jobId`.
4. RabbitMQ stores the job until a worker is available.
5. A worker receives one job at a time because `prefetchCount` is set to `1`.
6. Worker generates the report artifact.
7. Worker sends `ack` when processing succeeds.
8. Worker sends `nack` when processing fails.
9. In production, failed jobs should route to a retry exchange or dead-letter queue.

Production hardening usually adds:

| Concern | Production Approach |
| --- | --- |
| Retries | Use delayed retries or retry exchanges. |
| Poison messages | Configure a dead-letter exchange and dead-letter queue. |
| Worker scaling | Scale workers horizontally and tune `prefetchCount`. |
| Durability | Use durable queues, persistent messages, and mirrored/quorum queues. |
| Idempotency | Make report generation safe if a job is redelivered. |
| Visibility | Monitor ready messages, unacked messages, consumer count, and processing time. |

## Functionality Flow

This is the exact local demo flow:

1. `POST /reports` receives JSON with `type`, `requestedBy`, `dateFrom`, `dateTo`, and optional `priority`.
2. `HttpLoggingMiddleware` creates or forwards `x-correlation-id` and logs request start.
3. `ValidationPipe` rejects invalid report requests.
4. `ReportsController` logs that report generation was accepted for background processing.
5. `ReportsService` creates a `jobId`, stores a Postgres-backed `queued` status, builds a `ReportJob`, and publishes it with pattern `report.generate`.
6. API returns `accepted: true`, the queue name, status URL, and job status record.
7. `ReportsWorker` receives the job from queue `report.jobs`.
8. Worker marks the job `processing` and logs delivery tag, redelivery status, job type, priority, and queue name.
9. Worker simulates report generation using `REPORT_WORK_MS`.
10. Worker logs the generated artifact path and marks the job `completed`.
11. Worker acknowledges the message with `channel.ack(message)`.
12. If `simulateFailure` is true, worker marks the job `failed`, sends `channel.nack(message, false, false)`, and logs the failure.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Confirms the API is running and shows the queue name. |
| `POST` | `/reports` | Enqueues a durable report generation job. |
| `GET` | `/reports` | Lists Postgres-backed job status records. |
| `GET` | `/reports/:jobId` | Returns the current status for one report job. |

Create a report job:

```bash
curl -X POST http://localhost:3002/reports \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-rabbit-1' \
  -d '{"type":"sales","requestedBy":"finance@example.com","dateFrom":"2026-01-01","dateTo":"2026-01-31","priority":"high"}'
```

Example response:

```json
{
  "accepted": true,
  "reason": "Report generation is queued because it is slow and should not block the HTTP request.",
  "queue": "report.jobs",
  "statusUrl": "/reports/uuid",
  "job": {
    "jobId": "uuid",
    "status": "queued",
    "attempts": 0,
    "job": {
      "jobId": "uuid",
      "type": "sales",
      "requestedBy": "finance@example.com",
      "priority": "high",
      "simulateFailure": false,
      "correlationId": "demo-rabbit-1"
    }
  }
}
```

Query job status:

```bash
curl http://localhost:3002/reports/<jobId>
```

Simulate a failed job:

```bash
curl -X POST http://localhost:3002/reports \
  -H 'content-type: application/json' \
  -d '{"type":"compliance","requestedBy":"audit@example.com","dateFrom":"2026-01-01","dateTo":"2026-01-31","priority":"critical","simulateFailure":true}'
```

The response contains a nested status record because the API returns the Postgres-backed production-style job state, not only the raw broker payload.

Raw broker job fields look like this inside `job.job`:

```json
{
  "jobId": "uuid",
  "type": "sales",
  "requestedBy": "finance@example.com",
  "priority": "high",
  "simulateFailure": false,
  "correlationId": "demo-rabbit-1"
}
```

## Run

Run only this service and RabbitMQ:

```bash
cd /home/hasnain/Desktop/Projects/nestjs-broker-problem-suite/rabbitmq-report-jobs
docker compose up --build
```

Run the full suite from the root folder:

```bash
cd /home/hasnain/Desktop/Projects/nestjs-broker-problem-suite
docker compose up --build
```

RabbitMQ Management UI:

| URL | Username | Password |
| --- | --- | --- |
| `http://localhost:15672` | `guest` | `guest` |

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `3002` | HTTP port. |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672` | RabbitMQ connection URL. |
| `REPORT_QUEUE` | `report.jobs` | Queue used for report jobs. |
| `REPORT_PATTERN` | `report.generate` | Nest message pattern. |
| `REPORT_WORK_MS` | `400` | Simulated report generation time. |
| `POSTGRES_HOST` | `localhost` | Postgres host. |
| `POSTGRES_PORT` | `5432` | Postgres port. |
| `POSTGRES_USER` | `broker_suite` | Postgres username. |
| `POSTGRES_PASSWORD` | `broker_suite` | Postgres password. |
| `POSTGRES_DB` | `broker_suite` | Postgres database. |
| `POSTGRES_POOL_MAX` | `10` | Max database pool connections. |

## Logging

Important log events:

| Log Message | Meaning |
| --- | --- |
| `Publishing report job to RabbitMQ` | Producer is enqueueing a job. |
| `Report job status recorded as queued` | Local status store created a queued record. |
| `Report job enqueued` | Publish call completed from the API perspective. |
| `RabbitMQ report job received` | Worker received a message and logs delivery metadata. |
| `Report artifact generated` | Simulated report output was produced. |
| `RabbitMQ report job acknowledged` | Worker acked the message successfully. |
| `RabbitMQ report job failed and was nacked` | Worker rejected a failed message. |
