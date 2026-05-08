import { PresenceStatus } from './presence.dto';

export interface PresenceChangedEvent {
  eventId: string;
  userId: string;
  status: PresenceStatus;
  roomId?: string;
  deviceId?: string;
  changedAt: string;
  correlationId: string;
}
