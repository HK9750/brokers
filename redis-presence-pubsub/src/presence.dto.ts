import { IsEnum, IsOptional, IsString } from 'class-validator';

export enum PresenceStatus {
  Online = 'online',
  Away = 'away',
  Typing = 'typing',
  Offline = 'offline',
}

export class PublishPresenceDto {
  @IsString()
  userId!: string;

  @IsEnum(PresenceStatus)
  status!: PresenceStatus;

  @IsOptional()
  @IsString()
  roomId?: string;

  @IsOptional()
  @IsString()
  deviceId?: string;
}
