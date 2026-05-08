export const REDIS_PUBSUB_CLIENT = 'REDIS_PUBSUB_CLIENT';
export const PRESENCE_PATTERN = process.env.PRESENCE_PATTERN ?? 'presence.changed';

export function getPort(): number {
  return Number(process.env.PORT ?? 3004);
}

export function getRedisHost(): string {
  return process.env.REDIS_HOST ?? 'localhost';
}

export function getRedisPort(): number {
  return Number(process.env.REDIS_PORT ?? 6379);
}

export function getRedisPassword(): string | undefined {
  return process.env.REDIS_PASSWORD || undefined;
}

export function getRedisRetryAttempts(): number {
  return Number(process.env.REDIS_RETRY_ATTEMPTS ?? 20);
}

export function getRedisRetryDelayMs(): number {
  return Number(process.env.REDIS_RETRY_DELAY_MS ?? 1000);
}

export function getPresenceTtlMs(): number {
  return Number(process.env.PRESENCE_TTL_MS ?? 60_000);
}

export function getRedisTransportOptions(): {
  host: string;
  port: number;
  password?: string;
  retryAttempts: number;
  retryDelay: number;
} {
  return {
    host: getRedisHost(),
    port: getRedisPort(),
    password: getRedisPassword(),
    retryAttempts: getRedisRetryAttempts(),
    retryDelay: getRedisRetryDelayMs(),
  };
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
