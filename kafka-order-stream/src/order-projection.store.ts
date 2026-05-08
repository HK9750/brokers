import { Injectable, NotFoundException, OnModuleInit } from '@nestjs/common';
import { StructuredLogger } from './common/observability';
import { PostgresService } from './common/postgres.service';
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
export class OrderProjectionStore implements OnModuleInit {
  private readonly logger = new StructuredLogger(OrderProjectionStore.name);

  constructor(private readonly postgres: PostgresService) {}

  async onModuleInit(): Promise<void> {
    await this.postgres.query(`
      create table if not exists kafka_customer_order_projections (
        customer_id text primary key,
        order_count integer not null,
        high_risk_order_count integer not null,
        total_spend numeric(14, 2) not null,
        average_order_value numeric(14, 2) not null,
        last_order_id text not null,
        last_risk_score integer not null,
        last_kafka_offset text not null,
        last_kafka_partition integer not null,
        recent_orders jsonb not null,
        updated_at timestamptz not null
      )
    `);

    this.logger.log('Kafka projection table is ready', { table: 'kafka_customer_order_projections' });
  }

  async applyOrderCreated(
    event: OrderCreatedEvent,
    riskScore: number,
    metadata: KafkaProjectionMetadata,
  ): Promise<CustomerOrderProjection> {
    const existing = await this.findCustomerProjection(event.customerId);
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

    await this.postgres.query(
      `
        insert into kafka_customer_order_projections (
          customer_id,
          order_count,
          high_risk_order_count,
          total_spend,
          average_order_value,
          last_order_id,
          last_risk_score,
          last_kafka_offset,
          last_kafka_partition,
          recent_orders,
          updated_at
        ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
        on conflict (customer_id) do update set
          order_count = excluded.order_count,
          high_risk_order_count = excluded.high_risk_order_count,
          total_spend = excluded.total_spend,
          average_order_value = excluded.average_order_value,
          last_order_id = excluded.last_order_id,
          last_risk_score = excluded.last_risk_score,
          last_kafka_offset = excluded.last_kafka_offset,
          last_kafka_partition = excluded.last_kafka_partition,
          recent_orders = excluded.recent_orders,
          updated_at = excluded.updated_at
      `,
      [
        projection.customerId,
        projection.orderCount,
        projection.highRiskOrderCount,
        projection.totalSpend,
        projection.averageOrderValue,
        projection.lastOrderId,
        projection.lastRiskScore,
        projection.lastKafkaOffset,
        projection.lastKafkaPartition,
        JSON.stringify(projection.recentOrders),
        projection.updatedAt,
      ],
    );

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

  async getCustomerProjection(customerId: string): Promise<CustomerOrderProjection> {
    const projection = await this.findCustomerProjection(customerId);

    if (!projection) {
      throw new NotFoundException(`No Kafka projection exists for customer ${customerId} yet`);
    }

    return projection;
  }

  async listCustomerProjections(): Promise<CustomerOrderProjection[]> {
    const result = await this.postgres.query<ProjectionRow>(`
      select *
      from kafka_customer_order_projections
      order by updated_at desc
    `);

    return result.rows.map((row) => this.mapRow(row));
  }

  private async findCustomerProjection(customerId: string): Promise<CustomerOrderProjection | undefined> {
    const result = await this.postgres.query<ProjectionRow>(
      'select * from kafka_customer_order_projections where customer_id = $1',
      [customerId],
    );

    const row = result.rows[0];
    return row ? this.mapRow(row) : undefined;
  }

  private mapRow(row: ProjectionRow): CustomerOrderProjection {
    return {
      customerId: row.customer_id,
      orderCount: row.order_count,
      highRiskOrderCount: row.high_risk_order_count,
      totalSpend: Number(row.total_spend),
      averageOrderValue: Number(row.average_order_value),
      lastOrderId: row.last_order_id,
      lastRiskScore: row.last_risk_score,
      lastKafkaOffset: row.last_kafka_offset,
      lastKafkaPartition: row.last_kafka_partition,
      updatedAt: row.updated_at.toISOString(),
      recentOrders: row.recent_orders,
    };
  }
}

interface ProjectionRow extends Record<string, unknown> {
  customer_id: string;
  order_count: number;
  high_risk_order_count: number;
  total_spend: string;
  average_order_value: string;
  last_order_id: string;
  last_risk_score: number;
  last_kafka_offset: string;
  last_kafka_partition: number;
  recent_orders: ProjectedOrderSummary[];
  updated_at: Date;
}
