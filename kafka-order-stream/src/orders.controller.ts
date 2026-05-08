import { Body, Controller, Get, Param, Post, Req } from '@nestjs/common';
import { CorrelatedRequest, StructuredLogger } from './common/observability';
import { ORDER_CREATED_TOPIC } from './config';
import { OrderProjectionStore } from './order-projection.store';
import { CreateOrderDto } from './orders.dto';
import { OrdersService } from './orders.service';

@Controller()
export class OrdersController {
  private readonly logger = new StructuredLogger(OrdersController.name);

  constructor(
    private readonly ordersService: OrdersService,
    private readonly projectionStore: OrderProjectionStore,
  ) {}

  @Get('health')
  health(): Record<string, string> {
    return { status: 'ok', broker: 'kafka', topic: ORDER_CREATED_TOPIC };
  }

  @Post('orders')
  async createOrder(@Body() dto: CreateOrderDto, @Req() request: CorrelatedRequest): Promise<Record<string, unknown>> {
    const correlationId = request.correlationId ?? 'missing-correlation-id';
    const idempotencyKey = this.getSingleHeader(request, 'idempotency-key');

    this.logger.log('Order intake accepted for streaming', {
      correlationId,
      customerId: dto.customerId,
      itemCount: dto.items.length,
      idempotencyKey,
    });

    const result = await this.ordersService.createOrder(dto, correlationId, idempotencyKey);

    return {
      accepted: true,
      duplicate: result.duplicate,
      reason: 'Order was appended to the Kafka stream for downstream projections.',
      topic: ORDER_CREATED_TOPIC,
      projectionUrl: `/customers/${result.event.customerId}/projection`,
      event: result.event,
    };
  }

  @Get('customers/:customerId/projection')
  async getCustomerProjection(@Param('customerId') customerId: string): Promise<Record<string, unknown>> {
    return {
      projectionType: 'customer-order-history',
      projection: await this.projectionStore.getCustomerProjection(customerId),
    };
  }

  @Get('projections')
  async listProjections(): Promise<Record<string, unknown>> {
    const projections = await this.projectionStore.listCustomerProjections();

    return {
      count: projections.length,
      projections,
    };
  }

  private getSingleHeader(request: CorrelatedRequest, name: string): string | undefined {
    const value = request.headers[name];
    return Array.isArray(value) ? value[0] : value;
  }
}
