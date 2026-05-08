export const REPORT_QUEUE_CLIENT = 'REPORT_QUEUE_CLIENT';
export const REPORT_PATTERN = process.env.REPORT_PATTERN ?? 'report.generate';

export function getPort(): number {
  return Number(process.env.PORT ?? 3002);
}

export function getRabbitUrl(): string {
  return process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672';
}

export function getReportQueue(): string {
  return process.env.REPORT_QUEUE ?? 'report.jobs';
}

export function getReportWorkMs(): number {
  return Number(process.env.REPORT_WORK_MS ?? 400);
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
