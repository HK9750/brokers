import { IsEnum, IsNumber, IsObject, IsOptional, IsString } from 'class-validator';

export enum DeviceCommand {
  Lock = 'lock',
  Unlock = 'unlock',
  Reboot = 'reboot',
  Locate = 'locate',
}

export class SendDeviceCommandDto {
  @IsEnum(DeviceCommand)
  command!: DeviceCommand;

  @IsOptional()
  @IsObject()
  payload?: Record<string, unknown>;
}

export class PublishTelemetryDto {
  @IsString()
  metric!: string;

  @IsNumber()
  value!: number;

  @IsOptional()
  @IsString()
  unit?: string;
}

export class DeviceHeartbeatDto {
  @IsOptional()
  @IsString()
  firmwareVersion?: string;

  @IsOptional()
  @IsString()
  region?: string;
}
