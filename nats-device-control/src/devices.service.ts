import { Inject, Injectable, OnModuleDestroy, OnModuleInit, ServiceUnavailableException } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import { catchError, lastValueFrom, throwError, timeout } from 'rxjs';
import { StructuredLogger } from './common/observability';
import {
  DEVICE_COMMAND_PATTERN,
  DEVICE_TELEMETRY_PATTERN,
  NATS_BUS_CLIENT,
  getCommandTimeoutMs,
  getNatsServers,
} from './config';
import { DeviceCommandAck, DeviceCommandMessage, DeviceTelemetryEvent } from './device.contract';
import { DeviceState, DevicesRegistry } from './devices.registry';
import { DeviceHeartbeatDto, PublishTelemetryDto, SendDeviceCommandDto } from './devices.dto';

@Injectable()
export class DevicesService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new StructuredLogger(DevicesService.name);

  constructor(
    @Inject(NATS_BUS_CLIENT) private readonly client: ClientProxy,
    private readonly registry: DevicesRegistry,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.client.connect();
    this.logger.log('NATS client connected', {
      servers: getNatsServers(),
      commandPattern: DEVICE_COMMAND_PATTERN,
      telemetryPattern: DEVICE_TELEMETRY_PATTERN,
    });
  }

  onModuleDestroy(): void {
    this.client.close();
    this.logger.log('NATS client disconnected');
  }

  async sendCommand(deviceId: string, dto: SendDeviceCommandDto, correlationId: string): Promise<DeviceCommandAck> {
    const message: DeviceCommandMessage = {
      commandId: randomUUID(),
      deviceId,
      command: dto.command,
      payload: dto.payload,
      sentAt: new Date().toISOString(),
      correlationId,
    };

    this.logger.log('Sending device command over NATS request/reply', {
      correlationId,
      commandId: message.commandId,
      deviceId,
      command: dto.command,
      timeoutMs: getCommandTimeoutMs(),
    });

    const ack = await lastValueFrom(
      this.client.send<DeviceCommandAck>(DEVICE_COMMAND_PATTERN, message).pipe(
        timeout(getCommandTimeoutMs()),
        catchError((error: unknown) => {
          this.logger.error('NATS command request failed', error instanceof Error ? error.stack : undefined, {
            correlationId,
            commandId: message.commandId,
            deviceId,
            error,
          });
          return throwError(() => new ServiceUnavailableException('Device command bus timed out or failed'));
        }),
      ),
    );

    this.logger.log('Device command reply received from NATS', {
      correlationId,
      commandId: ack.commandId,
      deviceId: ack.deviceId,
      status: ack.status,
      processingMs: ack.processingMs,
    });
    this.registry.recordCommandAck(ack, dto.command);

    return ack;
  }

  recordHeartbeat(deviceId: string, dto: DeviceHeartbeatDto, correlationId: string): DeviceState {
    return this.registry.recordHeartbeat(deviceId, dto, correlationId);
  }

  getDevice(deviceId: string): DeviceState {
    return this.registry.getDevice(deviceId);
  }

  listDevices(): DeviceState[] {
    return this.registry.listDevices();
  }

  async publishTelemetry(deviceId: string, dto: PublishTelemetryDto, correlationId: string): Promise<DeviceTelemetryEvent> {
    const event: DeviceTelemetryEvent = {
      eventId: randomUUID(),
      deviceId,
      metric: dto.metric,
      value: dto.value,
      unit: dto.unit,
      observedAt: new Date().toISOString(),
      correlationId,
    };

    this.logger.log('Publishing device telemetry over NATS', {
      correlationId,
      eventId: event.eventId,
      deviceId,
      metric: event.metric,
      value: event.value,
    });

    await lastValueFrom(this.client.emit(DEVICE_TELEMETRY_PATTERN, event));

    return event;
  }
}
