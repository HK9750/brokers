import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { HttpLoggingMiddleware } from './common/observability';
import { ORDER_STREAM_CLIENT, getKafkaBrokers } from './config';
import { OrderEventsConsumer } from './order-events.consumer';
import { OrderProjectionStore } from './order-projection.store';
import { OrdersController } from './orders.controller';
import { OrdersService } from './orders.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: ORDER_STREAM_CLIENT,
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: process.env.KAFKA_CLIENT_ID ?? 'order-api',
            brokers: getKafkaBrokers(),
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [OrdersController, OrderEventsConsumer],
  providers: [OrdersService, OrderProjectionStore],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(HttpLoggingMiddleware).forRoutes('*');
  }
}
