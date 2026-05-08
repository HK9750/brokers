import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { StructuredLogger } from './common/observability';
import { ORDER_CREATED_TOPIC } from './config';
import { OrderCreatedEvent } from './order-events.contract';
import { OrderProjectionStore } from './order-projection.store';

type KafkaPayload<T> = T | { value?: T };

@Controller()
export class OrderEventsConsumer {
  private readonly logger = new StructuredLogger(OrderEventsConsumer.name);

  constructor(private readonly projectionStore: OrderProjectionStore) {}

  @EventPattern(ORDER_CREATED_TOPIC)
  handleOrderCreated(@Payload() payload: KafkaPayload<OrderCreatedEvent>, @Ctx() context: KafkaContext): void {
    const event = this.unwrapPayload(payload);
    const message = context.getMessage();
    const riskScore = this.calculateRiskScore(event);
    const brokerMetadata = {
      topic: context.getTopic(),
      partition: context.getPartition(),
      offset: message.offset,
    };
    const metadata = {
      correlationId: event.correlationId,
      eventId: event.eventId,
      orderId: event.orderId,
      customerId: event.customerId,
      total: event.total,
      riskScore,
      ...brokerMetadata,
    };

    this.logger.log('Kafka order event consumed for projection', metadata);
    const projection = this.projectionStore.applyOrderCreated(event, riskScore, brokerMetadata);

    if (riskScore >= 80) {
      this.logger.warn('High-risk order flagged for manual review', {
        ...metadata,
        reason: 'High value order or coupon anomaly detected in the event stream.',
      });
      return;
    }

    this.logger.log('Order projected into customer timeline', {
      ...metadata,
      projection: 'customer-order-history',
      orderCount: projection.orderCount,
      totalSpend: projection.totalSpend,
    });
  }

  private unwrapPayload(payload: KafkaPayload<OrderCreatedEvent>): OrderCreatedEvent {
    if (payload && typeof payload === 'object' && 'value' in payload && payload.value) {
      return payload.value;
    }

    return payload as OrderCreatedEvent;
  }

  private calculateRiskScore(event: OrderCreatedEvent): number {
    let score = 10;

    if (event.total > 1_000) {
      score += 55;
    }

    if (event.items.length > 5) {
      score += 20;
    }

    if (event.couponCode?.toLowerCase().includes('vip')) {
      score += 15;
    }

    return Math.min(score, 100);
  }
}
