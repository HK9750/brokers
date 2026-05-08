# NATS Device Control

This service demonstrates NATS as a low-latency messaging layer. The business example is device control: HTTP sends a command to a device-control worker using request-reply, and telemetry is published as lightweight fanout.

## Architecture

```text
Command flow

HTTP client
  |
  | POST /devices/:deviceId/commands
  v
NestJS DevicesController
  |
  | sends request on subject device.command
  v
NATS server
  |
  | delivers to one member of queue group device-control-workers
  v
DevicesResponder
  |
  | accepts or rejects command
  v
Reply returned through NATS and then HTTP

Telemetry flow

HTTP client
  |
  | POST /devices/:deviceId/telemetry
  v
DevicesService
  |
  | emits event on subject device.telemetry
  v
NATS server
  |
  | fanout to telemetry subscribers
  v
DevicesResponder telemetry handler
```

| Component | File | Responsibility |
| --- | --- | --- |
| HTTP entrypoint | `src/devices.controller.ts` | Exposes command, telemetry, and health endpoints. |
| NATS client service | `src/devices.service.ts` | Sends request-reply commands and emits telemetry events. |
| NATS responder | `src/devices.responder.ts` | Handles command replies and telemetry events. |
| Contract | `src/device.contract.ts` | Defines command, acknowledgement, and telemetry payloads. |
| Validation | `src/devices.dto.ts` | Validates allowed commands and telemetry payloads. |
| Broker config | `src/config.ts` | Reads NATS URL, queue group, timeout, and port. |
| Observability | `src/common/observability.ts` | Adds JSON logging, correlation IDs, HTTP logs, and exception logs. |

NATS is used because command/control traffic should be fast. The caller needs a quick acknowledgement, not a long durable event history.

## Production Flow

In a production system, device control is usually split into gateway, control, and telemetry services:

| Production Unit | Role |
| --- | --- |
| `device-api` | Accepts command requests from users or internal systems. |
| NATS cluster | Routes commands and telemetry with very low latency. |
| `device-control-worker` queue group | Handles commands and returns request-reply acknowledgements. |
| `device-session-gateway` | Maintains active device sessions over WebSocket, MQTT, TCP, or another protocol. |
| `telemetry-service` | Consumes telemetry and raises alerts. |

Production command flow:

1. Client sends a command such as `reboot`, `lock`, `unlock`, or `locate`.
2. API validates the command and sends a NATS request on subject `device.command`.
3. NATS routes the request to one active worker in queue group `device-control-workers`.
4. Worker checks whether the device can accept the command.
5. Worker replies with `accepted` or `rejected`.
6. API returns the reply to the HTTP client.
7. If no reply arrives before timeout, API returns service unavailable.

Production telemetry flow:

1. Device or gateway publishes telemetry.
2. API emits telemetry on subject `device.telemetry`.
3. Multiple subscribers can consume telemetry for alerts, live dashboards, or metrics.
4. If durable telemetry is required, NATS JetStream should be used instead of core NATS.

Production hardening usually adds:

| Concern | Production Approach |
| --- | --- |
| Durability | Use JetStream for commands or telemetry that must not be lost. |
| Timeouts | Set command-specific request timeouts and return clear failure responses. |
| Worker scaling | Scale queue-group workers horizontally. |
| Device state | Track last heartbeat and session ownership outside the command handler. |
| Security | Add authentication, authorization, and per-device command permissions. |
| Observability | Monitor request latency, timeout count, rejected commands, and subscriber count. |

## Functionality Flow

This is the exact local command flow:

1. `POST /devices/:deviceId/heartbeat` records a fresh heartbeat in `DevicesRegistry`.
2. `POST /devices/:deviceId/commands` receives JSON with `command` and optional `payload`.
3. `HttpLoggingMiddleware` creates or forwards `x-correlation-id` and logs request start.
4. `ValidationPipe` rejects unknown commands.
5. `DevicesController` logs that the command was accepted by HTTP.
6. `DevicesService` creates a `commandId` and sends a NATS request to subject `device.command`.
7. `DevicesResponder` receives the command as part of queue group `device-control-workers`.
8. Responder checks `DevicesRegistry` and rejects commands for devices with no fresh heartbeat.
9. If command is `reboot`, responder logs a warning because reboot needs session draining.
10. Responder returns a command acknowledgement over NATS.
11. `DevicesService` receives the reply, updates command state, and returns it over HTTP.

This is the exact local telemetry flow:

1. `POST /devices/:deviceId/telemetry` receives JSON with `metric`, `value`, and optional `unit`.
2. `DevicesService` creates an `eventId` and emits a NATS event to subject `device.telemetry`.
3. API returns `accepted: true` immediately after the emit call.
4. `DevicesResponder` consumes the telemetry event.
5. If metric is `temperature` and value is greater than `40`, responder logs a threshold warning.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Confirms the API is running and shows the NATS queue group. |
| `GET` | `/devices` | Lists local device control-plane state. |
| `GET` | `/devices/:deviceId` | Returns heartbeat, command, and telemetry state for one device. |
| `POST` | `/devices/:deviceId/heartbeat` | Marks a device online so commands can be accepted. |
| `POST` | `/devices/:deviceId/commands` | Sends a request-reply command over NATS. |
| `POST` | `/devices/:deviceId/telemetry` | Publishes a telemetry event over NATS. |

Record a heartbeat before sending a command:

```bash
curl -X POST http://localhost:3003/devices/device-17/heartbeat \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-nats-heartbeat-1' \
  -d '{"firmwareVersion":"1.4.2","region":"warehouse-a"}'
```

Send a command:

```bash
curl -X POST http://localhost:3003/devices/device-17/commands \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-nats-1' \
  -d '{"command":"reboot","payload":{"reason":"firmware-upgrade"}}'
```

Publish telemetry:

```bash
curl -X POST http://localhost:3003/devices/device-17/telemetry \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-nats-telemetry-1' \
  -d '{"metric":"temperature","value":41.7,"unit":"celsius"}'
```

Example command response:

```json
{
  "accepted": true,
  "reason": "NATS is used here for low-latency command request/reply.",
  "reply": {
    "commandId": "uuid",
    "deviceId": "device-17",
    "status": "accepted",
    "processingMs": 25.2,
    "correlationId": "demo-nats-1"
  }
}
```

## Run

Run only this service and NATS:

```bash
cd /home/hasnain/Desktop/Projects/nestjs-broker-problem-suite/nats-device-control
docker compose up --build
```

Run the full suite from the root folder:

```bash
cd /home/hasnain/Desktop/Projects/nestjs-broker-problem-suite
docker compose up --build
```

NATS monitoring endpoint:

```text
http://localhost:8222
```

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `3003` | HTTP port. |
| `NATS_URL` | `nats://localhost:4222` | NATS server URL. |
| `NATS_QUEUE_GROUP` | `device-control-workers` | Queue group for command handlers. |
| `DEVICE_COMMAND_TIMEOUT_MS` | `1500` | HTTP command timeout window. |
| `DEVICE_HEARTBEAT_TTL_MS` | `30000` | How long a heartbeat stays fresh for command gating. |

## Logging

Important log events:

| Log Message | Meaning |
| --- | --- |
| `Sending device command over NATS request/reply` | API sent a command request. |
| `Device heartbeat recorded` | Device is marked online in local control-plane state. |
| `NATS command received by device control worker` | Worker received the command. |
| `NATS command rejected by device heartbeat gate` | Command was rejected because the device was unknown or stale. |
| `Reboot command requires device session drain` | Demo business warning for reboot. |
| `NATS command reply produced` | Worker replied to the request. |
| `Device command reply received from NATS` | API received the reply. |
| `NATS telemetry event consumed` | Telemetry subscriber received the event. |
| `Device telemetry threshold crossed` | Telemetry value crossed the demo threshold. |
