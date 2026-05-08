import { Injectable, NotFoundException, OnModuleInit } from '@nestjs/common';
import { StructuredLogger } from './common/observability';
import { PostgresService } from './common/postgres.service';
import { getHeartbeatTtlMs } from './config';
import { DeviceCommandAck, DeviceTelemetryEvent } from './device.contract';
import { DeviceCommand, DeviceHeartbeatDto } from './devices.dto';

export interface DeviceState {
  deviceId: string;
  status: 'online' | 'offline' | 'stale';
  lastHeartbeatAt?: string;
  lastHeartbeatMetadata?: DeviceHeartbeatDto;
  lastCommand?: {
    commandId: string;
    command?: DeviceCommand;
    status: DeviceCommandAck['status'];
    reason?: string;
    processedAt: string;
  };
  lastTelemetry?: DeviceTelemetryEvent;
  commandCount: number;
  telemetryCount: number;
  updatedAt: string;
}

@Injectable()
export class DevicesRegistry implements OnModuleInit {
  private readonly logger = new StructuredLogger(DevicesRegistry.name);

  constructor(private readonly postgres: PostgresService) {}

  async onModuleInit(): Promise<void> {
    await this.postgres.query(`
      create table if not exists nats_device_states (
        device_id text primary key,
        status text not null,
        last_heartbeat_at timestamptz,
        last_heartbeat_metadata jsonb,
        last_command jsonb,
        last_telemetry jsonb,
        command_count integer not null default 0,
        telemetry_count integer not null default 0,
        updated_at timestamptz not null
      )
    `);

    this.logger.log('NATS device state table is ready', { table: 'nats_device_states' });
  }

  async recordHeartbeat(deviceId: string, dto: DeviceHeartbeatDto, correlationId: string): Promise<DeviceState> {
    const existing = await this.findDevice(deviceId);
    const now = new Date().toISOString();
    const state: DeviceState = {
      deviceId,
      status: 'online',
      lastHeartbeatAt: now,
      lastHeartbeatMetadata: dto,
      lastCommand: existing?.lastCommand,
      lastTelemetry: existing?.lastTelemetry,
      commandCount: existing?.commandCount ?? 0,
      telemetryCount: existing?.telemetryCount ?? 0,
      updatedAt: now,
    };

    await this.upsertState(state);
    this.logger.log('Device heartbeat recorded', {
      correlationId,
      deviceId,
      status: state.status,
      firmwareVersion: dto.firmwareVersion,
      region: dto.region,
    });

    return state;
  }

  async recordCommandAck(ack: DeviceCommandAck, command?: DeviceCommand): Promise<DeviceState> {
    const existing = (await this.findDevice(ack.deviceId)) ?? this.emptyState(ack.deviceId);
    const state: DeviceState = {
      ...existing,
      status: ack.status === 'accepted' ? this.resolveStatus(existing) : existing.status,
      lastCommand: {
        commandId: ack.commandId,
        command,
        status: ack.status,
        reason: ack.reason,
        processedAt: ack.processedAt,
      },
      commandCount: existing.commandCount + 1,
      updatedAt: new Date().toISOString(),
    };

    await this.upsertState(state);
    return state;
  }

  async recordTelemetry(event: DeviceTelemetryEvent): Promise<DeviceState> {
    const existing = (await this.findDevice(event.deviceId)) ?? this.emptyState(event.deviceId);
    const state: DeviceState = {
      ...existing,
      status: this.resolveStatus(existing),
      lastTelemetry: event,
      telemetryCount: existing.telemetryCount + 1,
      updatedAt: new Date().toISOString(),
    };

    await this.upsertState(state);
    return state;
  }

  async getDevice(deviceId: string): Promise<DeviceState> {
    const existing = await this.findDevice(deviceId);

    if (!existing) {
      throw new NotFoundException(`Device ${deviceId} has no heartbeat, command, or telemetry state yet`);
    }

    const status = this.resolveStatus(existing);
    const state = status === existing.status ? existing : { ...existing, status, updatedAt: new Date().toISOString() };
    await this.upsertState(state);
    return state;
  }

  async listDevices(): Promise<DeviceState[]> {
    const result = await this.postgres.query<DeviceStateRow>('select * from nats_device_states order by updated_at desc');
    const devices: DeviceState[] = [];

    for (const row of result.rows) {
      devices.push(await this.getDevice(row.device_id));
    }

    return devices;
  }

  async canAcceptCommand(deviceId: string): Promise<{ accepted: boolean; reason?: string; state?: DeviceState }> {
    const state = await this.findDevice(deviceId);

    if (!state?.lastHeartbeatAt) {
      return { accepted: false, reason: 'Device has not sent a heartbeat to this control plane yet', state };
    }

    const resolved = await this.getDevice(deviceId);

    if (resolved.status !== 'online') {
      return { accepted: false, reason: `Device is ${resolved.status}`, state: resolved };
    }

    return { accepted: true, state: resolved };
  }

  private resolveStatus(state: DeviceState): DeviceState['status'] {
    if (!state.lastHeartbeatAt) {
      return state.status;
    }

    const ageMs = Date.now() - new Date(state.lastHeartbeatAt).getTime();
    return ageMs > getHeartbeatTtlMs() ? 'stale' : 'online';
  }

  private emptyState(deviceId: string): DeviceState {
    return {
      deviceId,
      status: 'offline',
      commandCount: 0,
      telemetryCount: 0,
      updatedAt: new Date().toISOString(),
    };
  }

  private async findDevice(deviceId: string): Promise<DeviceState | undefined> {
    const result = await this.postgres.query<DeviceStateRow>('select * from nats_device_states where device_id = $1', [deviceId]);
    const row = result.rows[0];

    return row ? this.mapRow(row) : undefined;
  }

  private async upsertState(state: DeviceState): Promise<void> {
    await this.postgres.query(
      `
        insert into nats_device_states (
          device_id,
          status,
          last_heartbeat_at,
          last_heartbeat_metadata,
          last_command,
          last_telemetry,
          command_count,
          telemetry_count,
          updated_at
        ) values ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7, $8, $9)
        on conflict (device_id) do update set
          status = excluded.status,
          last_heartbeat_at = excluded.last_heartbeat_at,
          last_heartbeat_metadata = excluded.last_heartbeat_metadata,
          last_command = excluded.last_command,
          last_telemetry = excluded.last_telemetry,
          command_count = excluded.command_count,
          telemetry_count = excluded.telemetry_count,
          updated_at = excluded.updated_at
      `,
      [
        state.deviceId,
        state.status,
        state.lastHeartbeatAt ?? null,
        state.lastHeartbeatMetadata ? JSON.stringify(state.lastHeartbeatMetadata) : null,
        state.lastCommand ? JSON.stringify(state.lastCommand) : null,
        state.lastTelemetry ? JSON.stringify(state.lastTelemetry) : null,
        state.commandCount,
        state.telemetryCount,
        state.updatedAt,
      ],
    );
  }

  private mapRow(row: DeviceStateRow): DeviceState {
    return {
      deviceId: row.device_id,
      status: row.status,
      lastHeartbeatAt: row.last_heartbeat_at?.toISOString(),
      lastHeartbeatMetadata: row.last_heartbeat_metadata ?? undefined,
      lastCommand: row.last_command ?? undefined,
      lastTelemetry: row.last_telemetry ?? undefined,
      commandCount: row.command_count,
      telemetryCount: row.telemetry_count,
      updatedAt: row.updated_at.toISOString(),
    };
  }
}

interface DeviceStateRow extends Record<string, unknown> {
  device_id: string;
  status: DeviceState['status'];
  last_heartbeat_at: Date | null;
  last_heartbeat_metadata: DeviceHeartbeatDto | null;
  last_command: DeviceState['lastCommand'] | null;
  last_telemetry: DeviceTelemetryEvent | null;
  command_count: number;
  telemetry_count: number;
  updated_at: Date;
}
