export const NATS_BUS_CLIENT = 'NATS_BUS_CLIENT';
export const DEVICE_COMMAND_PATTERN = 'device.command';
export const DEVICE_TELEMETRY_PATTERN = 'device.telemetry';

export function getPort(): number {
  return Number(process.env.PORT ?? 3003);
}

export function getNatsServers(): string[] {
  return (process.env.NATS_URL ?? 'nats://localhost:4222')
    .split(',')
    .map((server) => server.trim())
    .filter(Boolean);
}

export function getNatsQueueGroup(): string {
  return process.env.NATS_QUEUE_GROUP ?? 'device-control-workers';
}

export function getCommandTimeoutMs(): number {
  return Number(process.env.DEVICE_COMMAND_TIMEOUT_MS ?? 1500);
}

export function getHeartbeatTtlMs(): number {
  return Number(process.env.DEVICE_HEARTBEAT_TTL_MS ?? 30_000);
}

export function getPostgresConfig(): {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  max: number;
} {
  return {
    host: process.env.POSTGRES_HOST ?? 'localhost',
    port: Number(process.env.POSTGRES_PORT ?? 5432),
    user: process.env.POSTGRES_USER ?? 'broker_suite',
    password: process.env.POSTGRES_PASSWORD ?? 'broker_suite',
    database: process.env.POSTGRES_DB ?? 'broker_suite',
    max: Number(process.env.POSTGRES_POOL_MAX ?? 10),
  };
}
