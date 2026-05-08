import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { HttpLoggingMiddleware } from './common/observability';
import { NATS_BUS_CLIENT, getNatsServers } from './config';
import { DevicesController } from './devices.controller';
import { DevicesRegistry } from './devices.registry';
import { DevicesResponder } from './devices.responder';
import { DevicesService } from './devices.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: NATS_BUS_CLIENT,
        transport: Transport.NATS,
        options: {
          servers: getNatsServers(),
        },
      },
    ]),
  ],
  controllers: [DevicesController, DevicesResponder],
  providers: [DevicesService, DevicesRegistry],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(HttpLoggingMiddleware).forRoutes('*');
  }
}
