import { Injectable, NotFoundException } from '@nestjs/common';
import { StructuredLogger } from './common/observability';
import { getPresenceTtlMs } from './config';
import { PresenceChangedEvent } from './presence.contract';
import { PresenceStatus } from './presence.dto';

export interface PresenceRecord {
  userId: string;
  status: PresenceStatus;
  roomId?: string;
  deviceId?: string;
  lastChangedAt: string;
  expiresAt: string;
  lastEventId: string;
}

export interface RoomPresenceRecord {
  roomId: string;
  onlineUsers: PresenceRecord[];
  totalOnline: number;
}

@Injectable()
export class PresenceStore {
  private readonly logger = new StructuredLogger(PresenceStore.name);
  private readonly users = new Map<string, PresenceRecord>();

  applyPresenceChanged(event: PresenceChangedEvent): PresenceRecord {
    const ttlMs = getPresenceTtlMs();
    const record: PresenceRecord = {
      userId: event.userId,
      status: event.status,
      roomId: event.roomId,
      deviceId: event.deviceId,
      lastChangedAt: event.changedAt,
      expiresAt: new Date(Date.now() + ttlMs).toISOString(),
      lastEventId: event.eventId,
    };

    this.users.set(event.userId, record);
    this.logger.log('Presence state updated from Redis Pub/Sub event', {
      correlationId: event.correlationId,
      eventId: event.eventId,
      userId: event.userId,
      status: event.status,
      roomId: event.roomId,
      ttlMs,
    });

    return record;
  }

  getUserPresence(userId: string): PresenceRecord {
    this.expireStaleRecords();
    const record = this.users.get(userId);

    if (!record) {
      throw new NotFoundException(`No current presence state exists for user ${userId}`);
    }

    return record;
  }

  listPresence(): PresenceRecord[] {
    this.expireStaleRecords();
    return [...this.users.values()].sort((left, right) => right.lastChangedAt.localeCompare(left.lastChangedAt));
  }

  getRoomPresence(roomId: string): RoomPresenceRecord {
    const onlineUsers = this.listPresence().filter(
      (record) => record.roomId === roomId && record.status !== PresenceStatus.Offline,
    );

    return {
      roomId,
      onlineUsers,
      totalOnline: onlineUsers.length,
    };
  }

  private expireStaleRecords(): void {
    const now = Date.now();

    for (const [userId, record] of this.users.entries()) {
      if (record.status !== PresenceStatus.Offline && new Date(record.expiresAt).getTime() < now) {
        this.users.set(userId, {
          ...record,
          status: PresenceStatus.Offline,
          lastChangedAt: new Date().toISOString(),
        });
      }
    }
  }
}
