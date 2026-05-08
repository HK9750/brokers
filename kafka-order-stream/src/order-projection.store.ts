import { Injectable, NotFoundException } from '@nestjs/common';
import { StructuredLogger } from './common/observability';
import { OrderCreatedEvent } from './order-events.contract';

export interface ProjectedOrderSummary {
  orderId: string;
  eventId: string;
  total: number;
  riskScore: number;
  createdAt: string;
  topic: string;
  partition: number;
  offset: string;
}

export interface CustomerOrderProjection {
  customerId: string;
  orderCount: number;
  highRiskOrderCount: number;
  totalSpend: number;
  averageOrderValue: number;
  lastOrderId: string;
  lastRiskScore: number;
  lastKafkaOffset: string;
  lastKafkaPartition: number;
  updatedAt: string;
  recentOrders: ProjectedOrderSummary[];
}

export interface KafkaProjectionMetadata {
  topic: string;
  partition: number;
  offset: string;
}

@Injectable()
export class OrderProjectionStore {
  private readonly logger = new StructuredLogger(OrderProjectionStore.name);
  private readonly projections = new Map<string, CustomerOrderProjection>();

  applyOrderCreated(event: OrderCreatedEvent, riskScore: number, metadata: KafkaProjectionMetadata): CustomerOrderProjection {
    const existing = this.projections.get(event.customerId);
    const orderCount = (existing?.orderCount ?? 0) + 1;
    const totalSpend = Number(((existing?.totalSpend ?? 0) + event.total).toFixed(2));
    const recentOrders = [
      {
        orderId: event.orderId,
        eventId: event.eventId,
        total: event.total,
        riskScore,
        createdAt: event.createdAt,
        ...metadata,
      },
      ...(existing?.recentOrders ?? []),
    ].slice(0, 10);

    const projection: CustomerOrderProjection = {
      customerId: event.customerId,
      orderCount,
      highRiskOrderCount: (existing?.highRiskOrderCount ?? 0) + (riskScore >= 80 ? 1 : 0),
      totalSpend,
      averageOrderValue: Number((totalSpend / orderCount).toFixed(2)),
      lastOrderId: event.orderId,
      lastRiskScore: riskScore,
      lastKafkaOffset: metadata.offset,
      lastKafkaPartition: metadata.partition,
      updatedAt: new Date().toISOString(),
      recentOrders,
    };

    this.projections.set(event.customerId, projection);
    this.logger.log('Customer projection updated from Kafka stream', {
      correlationId: event.correlationId,
      customerId: event.customerId,
      orderCount: projection.orderCount,
      totalSpend: projection.totalSpend,
      highRiskOrderCount: projection.highRiskOrderCount,
      kafkaOffset: metadata.offset,
    });

    return projection;
  }

  getCustomerProjection(customerId: string): CustomerOrderProjection {
    const projection = this.projections.get(customerId);

    if (!projection) {
      throw new NotFoundException(`No Kafka projection exists for customer ${customerId} yet`);
    }

    return projection;
  }

  listCustomerProjections(): CustomerOrderProjection[] {
    return [...this.projections.values()].sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
  }
}
