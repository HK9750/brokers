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
