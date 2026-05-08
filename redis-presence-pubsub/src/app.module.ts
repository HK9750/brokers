import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { HttpLoggingMiddleware } from './common/observability';
import { PostgresService } from './common/postgres.service';
import { REDIS_PUBSUB_CLIENT, getRedisTransportOptions } from './config';
import { PresenceController } from './presence.controller';
import { PresenceListener } from './presence.listener';
import { PresenceService } from './presence.service';
import { PresenceStore } from './presence.store';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: REDIS_PUBSUB_CLIENT,
        transport: Transport.REDIS,
        options: getRedisTransportOptions(),
      },
    ]),
  ],
  controllers: [PresenceController, PresenceListener],
  providers: [PresenceService, PresenceStore, PostgresService],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(HttpLoggingMiddleware).forRoutes('*');
  }
}
