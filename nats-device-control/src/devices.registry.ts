import { Injectable, NotFoundException } from '@nestjs/common';
import { StructuredLogger } from './common/observability';
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
export class DevicesRegistry {
  private readonly logger = new StructuredLogger(DevicesRegistry.name);
  private readonly devices = new Map<string, DeviceState>();

  recordHeartbeat(deviceId: string, dto: DeviceHeartbeatDto, correlationId: string): DeviceState {
    const existing = this.devices.get(deviceId);
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

    this.devices.set(deviceId, state);
    this.logger.log('Device heartbeat recorded', {
      correlationId,
      deviceId,
      status: state.status,
      firmwareVersion: dto.firmwareVersion,
      region: dto.region,
    });

    return state;
  }

  recordCommandAck(ack: DeviceCommandAck, command?: DeviceCommand): DeviceState {
    const existing = this.devices.get(ack.deviceId) ?? this.emptyState(ack.deviceId);
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

    this.devices.set(ack.deviceId, state);
    return state;
  }

  recordTelemetry(event: DeviceTelemetryEvent): DeviceState {
    const existing = this.devices.get(event.deviceId) ?? this.emptyState(event.deviceId);
    const state: DeviceState = {
      ...existing,
      status: this.resolveStatus(existing),
      lastTelemetry: event,
      telemetryCount: existing.telemetryCount + 1,
      updatedAt: new Date().toISOString(),
    };

    this.devices.set(event.deviceId, state);
    return state;
  }

  getDevice(deviceId: string): DeviceState {
    const existing = this.devices.get(deviceId);

    if (!existing) {
      throw new NotFoundException(`Device ${deviceId} has no heartbeat, command, or telemetry state yet`);
    }

    const status = this.resolveStatus(existing);
    const state = status === existing.status ? existing : { ...existing, status, updatedAt: new Date().toISOString() };
    this.devices.set(deviceId, state);
    return state;
  }

  listDevices(): DeviceState[] {
    return [...this.devices.keys()].map((deviceId) => this.getDevice(deviceId));
  }

  canAcceptCommand(deviceId: string): { accepted: boolean; reason?: string; state?: DeviceState } {
    const state = this.devices.get(deviceId);

    if (!state?.lastHeartbeatAt) {
      return { accepted: false, reason: 'Device has not sent a heartbeat to this control plane yet', state };
    }

    const resolved = this.getDevice(deviceId);

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
}
