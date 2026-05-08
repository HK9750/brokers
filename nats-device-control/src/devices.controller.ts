import { Body, Controller, Get, Param, Post, Req } from '@nestjs/common';
import { CorrelatedRequest, StructuredLogger } from './common/observability';
import { getNatsQueueGroup } from './config';
import { DeviceHeartbeatDto, PublishTelemetryDto, SendDeviceCommandDto } from './devices.dto';
import { DevicesService } from './devices.service';

@Controller()
export class DevicesController {
  private readonly logger = new StructuredLogger(DevicesController.name);

  constructor(private readonly devicesService: DevicesService) {}

  @Get('health')
  health(): Record<string, string> {
    return { status: 'ok', broker: 'nats', queueGroup: getNatsQueueGroup() };
  }

  @Get('devices')
  listDevices(): Record<string, unknown> {
    const devices = this.devicesService.listDevices();

    return {
      count: devices.length,
      devices,
    };
  }

  @Get('devices/:deviceId')
  getDevice(@Param('deviceId') deviceId: string): Record<string, unknown> {
    return {
      device: this.devicesService.getDevice(deviceId),
    };
  }

  @Post('devices/:deviceId/heartbeat')
  heartbeat(
    @Param('deviceId') deviceId: string,
    @Body() dto: DeviceHeartbeatDto,
    @Req() request: CorrelatedRequest,
  ): Record<string, unknown> {
    const correlationId = request.correlationId ?? 'missing-correlation-id';
    const device = this.devicesService.recordHeartbeat(deviceId, dto, correlationId);

    return {
      accepted: true,
      reason: 'Heartbeat marks the device online so command workers can reject stale devices.',
      device,
    };
  }

  @Post('devices/:deviceId/commands')
  async sendCommand(
    @Param('deviceId') deviceId: string,
    @Body() dto: SendDeviceCommandDto,
    @Req() request: CorrelatedRequest,
  ): Promise<Record<string, unknown>> {
    const correlationId = request.correlationId ?? 'missing-correlation-id';

    this.logger.log('HTTP device command accepted', {
      correlationId,
      deviceId,
      command: dto.command,
    });

    const ack = await this.devicesService.sendCommand(deviceId, dto, correlationId);

    return {
      accepted: ack.status === 'accepted',
      reason: 'NATS is used here for low-latency command request/reply.',
      reply: ack,
    };
  }

  @Post('devices/:deviceId/telemetry')
  async publishTelemetry(
    @Param('deviceId') deviceId: string,
    @Body() dto: PublishTelemetryDto,
    @Req() request: CorrelatedRequest,
  ): Promise<Record<string, unknown>> {
    const correlationId = request.correlationId ?? 'missing-correlation-id';
    const event = await this.devicesService.publishTelemetry(deviceId, dto, correlationId);

    return {
      accepted: true,
      reason: 'Telemetry is published as lightweight NATS fanout.',
      event,
    };
  }
}
