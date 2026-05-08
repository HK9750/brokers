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
