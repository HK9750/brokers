import { Injectable, NotFoundException, OnModuleInit } from '@nestjs/common';
import { StructuredLogger } from './common/observability';
import { PostgresService } from './common/postgres.service';
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
export class PresenceStore implements OnModuleInit {
  private readonly logger = new StructuredLogger(PresenceStore.name);

  constructor(private readonly postgres: PostgresService) {}

  async onModuleInit(): Promise<void> {
    await this.postgres.query(`
      create table if not exists redis_presence_states (
        user_id text primary key,
        status text not null,
        room_id text,
        device_id text,
        last_changed_at timestamptz not null,
        expires_at timestamptz not null,
        last_event_id text not null
      )
    `);

    await this.postgres.query('create index if not exists redis_presence_states_room_idx on redis_presence_states (room_id)');
    this.logger.log('Redis presence state table is ready', { table: 'redis_presence_states' });
  }

  async applyPresenceChanged(event: PresenceChangedEvent): Promise<PresenceRecord> {
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

    await this.postgres.query(
      `
        insert into redis_presence_states (
          user_id,
          status,
          room_id,
          device_id,
          last_changed_at,
          expires_at,
          last_event_id
        ) values ($1, $2, $3, $4, $5, $6, $7)
        on conflict (user_id) do update set
          status = excluded.status,
          room_id = excluded.room_id,
          device_id = excluded.device_id,
          last_changed_at = excluded.last_changed_at,
          expires_at = excluded.expires_at,
          last_event_id = excluded.last_event_id
      `,
      [
        record.userId,
        record.status,
        record.roomId ?? null,
        record.deviceId ?? null,
        record.lastChangedAt,
        record.expiresAt,
        record.lastEventId,
      ],
    );

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

  async getUserPresence(userId: string): Promise<PresenceRecord> {
    await this.expireStaleRecords();
    const result = await this.postgres.query<PresenceRow>('select * from redis_presence_states where user_id = $1', [userId]);
    const record = result.rows[0] ? this.mapRow(result.rows[0]) : undefined;

    if (!record) {
      throw new NotFoundException(`No current presence state exists for user ${userId}`);
    }

    return record;
  }

  async listPresence(): Promise<PresenceRecord[]> {
    await this.expireStaleRecords();
    const result = await this.postgres.query<PresenceRow>('select * from redis_presence_states order by last_changed_at desc');
    return result.rows.map((row) => this.mapRow(row));
  }

  async getRoomPresence(roomId: string): Promise<RoomPresenceRecord> {
    await this.expireStaleRecords();
    const result = await this.postgres.query<PresenceRow>(
      `
        select *
        from redis_presence_states
        where room_id = $1 and status <> $2
        order by last_changed_at desc
      `,
      [roomId, PresenceStatus.Offline],
    );
    const onlineUsers = result.rows.map((row) => this.mapRow(row));

    return {
      roomId,
      onlineUsers,
      totalOnline: onlineUsers.length,
    };
  }

  private async expireStaleRecords(): Promise<void> {
    await this.postgres.query(
      `
        update redis_presence_states
        set status = $1, last_changed_at = now()
        where status <> $1 and expires_at < now()
      `,
      [PresenceStatus.Offline],
    );
  }

  private mapRow(row: PresenceRow): PresenceRecord {
    return {
      userId: row.user_id,
      status: row.status,
      roomId: row.room_id ?? undefined,
      deviceId: row.device_id ?? undefined,
      lastChangedAt: row.last_changed_at.toISOString(),
      expiresAt: row.expires_at.toISOString(),
      lastEventId: row.last_event_id,
    };
  }
}

interface PresenceRow extends Record<string, unknown> {
  user_id: string;
  status: PresenceStatus;
  room_id: string | null;
  device_id: string | null;
  last_changed_at: Date;
  expires_at: Date;
  last_event_id: string;
}
