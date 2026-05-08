export interface OrderItemEvent {
  sku: string;
  quantity: number;
  unitPrice: number;
}

export interface OrderCreatedEvent {
  eventType: 'order.created';
  schemaVersion: 1;
  eventId: string;
  orderId: string;
  customerId: string;
  items: OrderItemEvent[];
  total: number;
  couponCode?: string;
  idempotencyKey?: string;
  createdAt: string;
  correlationId: string;
}
