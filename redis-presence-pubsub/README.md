# Redis Presence Pub/Sub

This service demonstrates Redis Pub/Sub as ephemeral fanout. The business example is user presence: an HTTP API publishes online, away, typing, and offline events so active subscribers can update UI state immediately.

Redis Pub/Sub is intentionally not durable. If a subscriber is offline, it misses the message. That is acceptable for presence updates, but not acceptable for payments, orders, reports, or audit logs.

## Architecture

```text
HTTP client
  |
  | POST /presence
  v
NestJS PresenceController
  |
  | validates presence payload
  v
PresenceService
  |
  | emits presence.changed event
  v
Redis Pub/Sub channel: presence.changed
  |
  | active subscribers receive immediately
  v
PresenceListener
  |
  | logs room routing, badge fanout, and offline cleanup
  v
Live UI / WebSocket gateway behavior
```

| Component | File | Responsibility |
| --- | --- | --- |
| HTTP entrypoint | `src/presence.controller.ts` | Exposes `/presence` and `/health`. |
| Publisher service | `src/presence.service.ts` | Builds and publishes `PresenceChangedEvent`. |
| Subscriber | `src/presence.listener.ts` | Handles presence fanout and offline cleanup logs. |
| Contract | `src/presence.contract.ts` | Defines the presence event shape. |
| Validation | `src/presence.dto.ts` | Validates user ID, status, room ID, and device ID. |
| Broker config | `src/config.ts` | Reads Redis host, port, retry settings, channel, and HTTP port. |
| Database | `src/common/postgres.service.ts` | Owns the Postgres connection pool used by the current presence store. |
| Observability | `src/common/observability.ts` | Adds JSON logging, correlation IDs, HTTP logs, and exception logs. |

Redis Pub/Sub is used because presence is a live hint, not a permanent business fact. The latest state matters more than replaying every historical presence change.

## Production Flow

In a production chat or collaboration system, this flow would normally be part of a WebSocket gateway layer:

| Production Unit | Role |
| --- | --- |
| `presence-api` or `websocket-gateway` | Publishes presence changes when users connect, disconnect, or type. |
| Redis | Broadcasts ephemeral presence messages to active gateway instances. |
| Gateway subscribers | Receive presence changes and fan them out to connected clients. |
| Presence state store | Stores current online state with TTL, usually in Redis keys. |
| Client apps | Update badges, typing indicators, and room member lists. |

Production request flow:

1. User connects to a gateway or changes presence status.
2. Gateway publishes a presence event to Redis Pub/Sub.
3. All currently active gateway instances subscribed to the channel receive the event.
4. Each gateway sends the update to relevant WebSocket rooms or users.
5. Offline subscribers miss the event, which is acceptable because presence is temporary.
6. Current presence should be stored separately with TTL if clients need to query latest state.

Production hardening usually adds:

| Concern | Production Approach |
| --- | --- |
| Current state | Store `presence:userId` keys with TTL in Redis. |
| Fanout | Use room-specific routing in WebSocket gateways. |
| Missed events | Accept misses for ephemeral updates, or use Redis Streams if durability is needed. |
| Disconnects | Publish `offline` and expire presence keys on heartbeat timeout. |
| Scale | Run multiple gateway instances subscribed to the same channel. |
| Reliability | Use retry settings for startup DNS and Redis availability. |

## Functionality Flow

This is the exact local demo flow:

1. `POST /presence` receives JSON with `userId`, `status`, optional `roomId`, and optional `deviceId`.
2. `HttpLoggingMiddleware` creates or forwards `x-correlation-id` and logs request start.
3. `ValidationPipe` rejects invalid presence statuses.
4. `PresenceController` logs that the presence update was accepted for pub/sub fanout.
5. `PresenceService` creates an `eventId` and publishes `PresenceChangedEvent` to channel `presence.changed`.
6. API returns `accepted: true` with the event body.
7. `PresenceListener` receives the event from Redis Pub/Sub.
8. `PresenceStore` persists current user presence with TTL-style expiry metadata in Postgres table `redis_presence_states`.
9. Listener logs the received event with user, status, room, device, and correlation ID.
10. If `roomId` is present, listener logs room routing to `room:<roomId>`.
11. If status is `offline`, listener logs volatile session cleanup.
12. For other statuses, listener logs that presence badge fanout completed.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Confirms the API is running and shows the Redis Pub/Sub channel. |
| `GET` | `/presence` | Lists current Postgres-backed presence state. |
| `GET` | `/presence/:userId` | Returns current presence for one user. |
| `GET` | `/rooms/:roomId/presence` | Returns current online users in one room. |
| `POST` | `/presence` | Publishes an ephemeral presence event over Redis Pub/Sub. |

Publish online presence:

```bash
curl -X POST http://localhost:3004/presence \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-redis-1' \
  -d '{"userId":"user-88","status":"online","roomId":"support","deviceId":"browser-1"}'
```

Publish offline presence:

```bash
curl -X POST http://localhost:3004/presence \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-redis-2' \
  -d '{"userId":"user-88","status":"offline","roomId":"support","deviceId":"browser-1"}'
```

Example response:

```json
{
  "accepted": true,
  "reason": "Presence is ephemeral, so Redis Pub/Sub is used for immediate fanout without durability overhead.",
  "event": {
    "eventId": "uuid",
    "userId": "user-88",
    "status": "online",
    "roomId": "support",
    "deviceId": "browser-1",
    "correlationId": "demo-redis-1"
  }
}
```

## Run

Run only this service and Redis:

```bash
cd /home/hasnain/Desktop/Projects/nestjs-broker-problem-suite/redis-presence-pubsub
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
| `PORT` | `3004` | HTTP port. |
| `REDIS_HOST` | `localhost` | Redis host. |
| `REDIS_PORT` | `6379` | Redis port. |
| `REDIS_PASSWORD` | empty | Optional Redis password. |
| `REDIS_RETRY_ATTEMPTS` | `20` | Startup retry attempts for transient Redis/Docker DNS issues. |
| `REDIS_RETRY_DELAY_MS` | `1000` | Delay between Redis retry attempts. |
| `PRESENCE_TTL_MS` | `60000` | Local current-presence freshness window. |
| `PRESENCE_PATTERN` | `presence.changed` | Redis Pub/Sub channel. |
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
| `Publishing ephemeral presence event to Redis Pub/Sub` | API published the presence event. |
| `Presence state updated from Redis Pub/Sub event` | Postgres current-presence state was updated. |
| `Redis Pub/Sub presence event received` | Subscriber received the event. |
| `Presence event routed to room subscribers` | Event would be sent to a WebSocket room. |
| `Offline presence event triggers volatile session cleanup` | Offline cleanup flow was triggered. |
| `Presence badge fanout completed` | Non-offline presence update completed. |

## Important Redis Pub/Sub Note

Redis Pub/Sub is not a queue. It does not store messages for later delivery. If you need durable Redis-based messaging, use Redis Streams or BullMQ instead.
