export const ORDER_STREAM_CLIENT = 'ORDER_STREAM_CLIENT';
export const ORDER_CREATED_TOPIC = process.env.KAFKA_ORDER_TOPIC ?? 'orders.created';

export function getPort(): number {
  return Number(process.env.PORT ?? 3001);
}

export function getKafkaBrokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);
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
