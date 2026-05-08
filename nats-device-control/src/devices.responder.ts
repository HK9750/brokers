import { Controller } from '@nestjs/common';
import { EventPattern, MessagePattern, Payload } from '@nestjs/microservices';
import { StructuredLogger } from './common/observability';
import { DEVICE_COMMAND_PATTERN, DEVICE_TELEMETRY_PATTERN } from './config';
import { DeviceCommandAck, DeviceCommandMessage, DeviceTelemetryEvent } from './device.contract';
import { DevicesRegistry } from './devices.registry';
import { DeviceCommand } from './devices.dto';

@Controller()
export class DevicesResponder {
  private readonly logger = new StructuredLogger(DevicesResponder.name);

  constructor(private readonly registry: DevicesRegistry) {}

  @MessagePattern(DEVICE_COMMAND_PATTERN)
  async handleCommand(@Payload() message: DeviceCommandMessage): Promise<DeviceCommandAck> {
    const startedAt = process.hrtime.bigint();

    this.logger.log('NATS command received by device control worker', {
      correlationId: message.correlationId,
      commandId: message.commandId,
      deviceId: message.deviceId,
      command: message.command,
    });

    await new Promise((resolve) => setTimeout(resolve, 25));

    const commandGate = this.registry.canAcceptCommand(message.deviceId);

    if (!commandGate.accepted) {
      this.logger.warn('NATS command rejected by device heartbeat gate', {
        correlationId: message.correlationId,
        commandId: message.commandId,
        deviceId: message.deviceId,
        reason: commandGate.reason,
      });
      return this.reply(message, startedAt, 'rejected', commandGate.reason);
    }

    if (message.command === DeviceCommand.Reboot) {
      this.logger.warn('Reboot command requires device session drain', {
        correlationId: message.correlationId,
        commandId: message.commandId,
        deviceId: message.deviceId,
      });
    }

    return this.reply(message, startedAt, 'accepted');
  }

  @EventPattern(DEVICE_TELEMETRY_PATTERN)
  handleTelemetry(@Payload() event: DeviceTelemetryEvent): void {
    const deviceState = this.registry.recordTelemetry(event);
    this.logger.log('NATS telemetry event consumed', {
      correlationId: event.correlationId,
      eventId: event.eventId,
      deviceId: event.deviceId,
      metric: event.metric,
      value: event.value,
      unit: event.unit,
      telemetryCount: deviceState.telemetryCount,
    });

    if (event.metric === 'temperature' && event.value > 40) {
      this.logger.warn('Device telemetry threshold crossed', {
        correlationId: event.correlationId,
        eventId: event.eventId,
        deviceId: event.deviceId,
        threshold: 40,
        observedValue: event.value,
      });
    }
  }

  private reply(
    message: DeviceCommandMessage,
    startedAt: bigint,
    status: DeviceCommandAck['status'],
    reason?: string,
  ): DeviceCommandAck {
    const processingMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
    const ack: DeviceCommandAck = {
      commandId: message.commandId,
      deviceId: message.deviceId,
      status,
      reason,
      processedAt: new Date().toISOString(),
      processingMs: Number(processingMs.toFixed(2)),
      correlationId: message.correlationId,
    };

    this.logger.log('NATS command reply produced', ack);
    return ack;
  }
}
