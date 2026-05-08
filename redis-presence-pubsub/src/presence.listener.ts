import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { StructuredLogger } from './common/observability';
import { PRESENCE_PATTERN } from './config';
import { PresenceChangedEvent } from './presence.contract';
import { PresenceStatus } from './presence.dto';
import { PresenceStore } from './presence.store';

@Controller()
export class PresenceListener {
  private readonly logger = new StructuredLogger(PresenceListener.name);

  constructor(private readonly presenceStore: PresenceStore) {}

  @EventPattern(PRESENCE_PATTERN)
  handlePresenceChanged(@Payload() event: PresenceChangedEvent): void {
    const currentState = this.presenceStore.applyPresenceChanged(event);
    this.logger.log('Redis Pub/Sub presence event received', {
      correlationId: event.correlationId,
      eventId: event.eventId,
      userId: event.userId,
      status: event.status,
      roomId: event.roomId,
      deviceId: event.deviceId,
      expiresAt: currentState.expiresAt,
    });

    if (event.roomId) {
      this.logger.log('Presence event routed to room subscribers', {
        correlationId: event.correlationId,
        eventId: event.eventId,
        roomId: event.roomId,
        websocketRoom: `room:${event.roomId}`,
      });
    }

    if (event.status === PresenceStatus.Offline) {
      this.logger.warn('Offline presence event triggers volatile session cleanup', {
        correlationId: event.correlationId,
        eventId: event.eventId,
        userId: event.userId,
        deviceId: event.deviceId,
      });
      return;
    }

    this.logger.log('Presence badge fanout completed', {
      correlationId: event.correlationId,
      eventId: event.eventId,
      userId: event.userId,
      status: event.status,
    });
  }
}
