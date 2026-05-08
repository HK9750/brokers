import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import { lastValueFrom } from 'rxjs';
import { ORDER_CREATED_TOPIC, ORDER_STREAM_CLIENT, getKafkaBrokers } from './config';
import { StructuredLogger } from './common/observability';
import { CreateOrderDto } from './orders.dto';
import { OrderCreatedEvent } from './order-events.contract';

export interface CreateOrderResult {
  event: OrderCreatedEvent;
  duplicate: boolean;
}

@Injectable()
export class OrdersService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new StructuredLogger(OrdersService.name);
  private readonly idempotencyCache = new Map<string, OrderCreatedEvent>();

  constructor(@Inject(ORDER_STREAM_CLIENT) private readonly client: ClientKafka) {}

  async onModuleInit(): Promise<void> {
    await this.client.connect();
    this.logger.log('Kafka producer connected', {
      brokers: getKafkaBrokers(),
      topic: ORDER_CREATED_TOPIC,
    });
  }

  async onModuleDestroy(): Promise<void> {
    this.client.close();
    this.logger.log('Kafka producer disconnected');
  }

  async createOrder(dto: CreateOrderDto, correlationId: string, idempotencyKey?: string): Promise<CreateOrderResult> {
    if (idempotencyKey) {
      const existingEvent = this.idempotencyCache.get(idempotencyKey);

      if (existingEvent) {
        this.logger.warn('Duplicate order request resolved from idempotency cache', {
          correlationId,
          idempotencyKey,
          orderId: existingEvent.orderId,
          eventId: existingEvent.eventId,
        });

        return { event: existingEvent, duplicate: true };
      }
    }

    const total = dto.items.reduce((sum, item) => sum + item.quantity * item.unitPrice, 0);
    const event: OrderCreatedEvent = {
      eventType: 'order.created',
      schemaVersion: 1,
      eventId: randomUUID(),
      orderId: randomUUID(),
      customerId: dto.customerId,
      items: dto.items,
      total: Number(total.toFixed(2)),
      couponCode: dto.couponCode,
      idempotencyKey,
      createdAt: new Date().toISOString(),
      correlationId,
    };

    this.logger.log('Publishing order event to Kafka', {
      correlationId,
      eventId: event.eventId,
      orderId: event.orderId,
      customerId: event.customerId,
      total: event.total,
      topic: ORDER_CREATED_TOPIC,
      messageKey: event.customerId,
      idempotencyKey,
    });

    await lastValueFrom(
      this.client.emit(ORDER_CREATED_TOPIC, {
        key: event.customerId,
        value: event,
        headers: { correlationId },
      }),
    );

    this.logger.log('Order event committed to Kafka producer buffer', {
      correlationId,
      eventId: event.eventId,
      orderId: event.orderId,
      topic: ORDER_CREATED_TOPIC,
    });

    if (idempotencyKey) {
      this.idempotencyCache.set(idempotencyKey, event);
    }

    return { event, duplicate: false };
  }
}
