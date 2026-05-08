import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import { lastValueFrom } from 'rxjs';
import { StructuredLogger } from './common/observability';
import { PRESENCE_PATTERN, REDIS_PUBSUB_CLIENT, getRedisHost, getRedisPort } from './config';
import { PresenceChangedEvent } from './presence.contract';
import { PublishPresenceDto } from './presence.dto';

@Injectable()
export class PresenceService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new StructuredLogger(PresenceService.name);

  constructor(@Inject(REDIS_PUBSUB_CLIENT) private readonly client: ClientProxy) {}

  async onModuleInit(): Promise<void> {
    await this.client.connect();
    this.logger.log('Redis Pub/Sub client connected', {
      host: getRedisHost(),
      port: getRedisPort(),
      pattern: PRESENCE_PATTERN,
    });
  }

  onModuleDestroy(): void {
    this.client.close();
    this.logger.log('Redis Pub/Sub client disconnected');
  }

  async publishPresence(dto: PublishPresenceDto, correlationId: string): Promise<PresenceChangedEvent> {
    const event: PresenceChangedEvent = {
      eventId: randomUUID(),
      userId: dto.userId,
      status: dto.status,
      roomId: dto.roomId,
      deviceId: dto.deviceId,
      changedAt: new Date().toISOString(),
      correlationId,
    };

    this.logger.log('Publishing ephemeral presence event to Redis Pub/Sub', {
      correlationId,
      eventId: event.eventId,
      userId: event.userId,
      status: event.status,
      roomId: event.roomId,
      pattern: PRESENCE_PATTERN,
    });

    await lastValueFrom(this.client.emit(PRESENCE_PATTERN, event));

    return event;
  }
}
