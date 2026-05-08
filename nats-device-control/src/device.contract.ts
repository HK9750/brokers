import { DeviceCommand } from './devices.dto';

export interface DeviceCommandMessage {
  commandId: string;
  deviceId: string;
  command: DeviceCommand;
  payload?: Record<string, unknown>;
  sentAt: string;
  correlationId: string;
}

export interface DeviceCommandAck {
  commandId: string;
  deviceId: string;
  status: 'accepted' | 'rejected';
  reason?: string;
  processedAt: string;
  processingMs: number;
  correlationId: string;
}

export interface DeviceTelemetryEvent {
  eventId: string;
  deviceId: string;
  metric: string;
  value: number;
  unit?: string;
  observedAt: string;
  correlationId: string;
}
