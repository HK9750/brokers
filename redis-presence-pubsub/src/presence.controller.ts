import { Body, Controller, Get, Param, Post, Req } from '@nestjs/common';
import { CorrelatedRequest, StructuredLogger } from './common/observability';
import { PRESENCE_PATTERN } from './config';
import { PublishPresenceDto } from './presence.dto';
import { PresenceService } from './presence.service';
import { PresenceStore } from './presence.store';

@Controller()
export class PresenceController {
  private readonly logger = new StructuredLogger(PresenceController.name);

  constructor(
    private readonly presenceService: PresenceService,
    private readonly presenceStore: PresenceStore,
  ) {}

  @Get('health')
  health(): Record<string, string> {
    return { status: 'ok', broker: 'redis-pubsub', pattern: PRESENCE_PATTERN };
  }

  @Get('presence')
  listPresence(): Record<string, unknown> {
    const users = this.presenceStore.listPresence();

    return {
      count: users.length,
      users,
    };
  }

  @Get('presence/:userId')
  getUserPresence(@Param('userId') userId: string): Record<string, unknown> {
    return {
      presence: this.presenceStore.getUserPresence(userId),
    };
  }

  @Get('rooms/:roomId/presence')
  getRoomPresence(@Param('roomId') roomId: string): Record<string, unknown> {
    return {
      room: this.presenceStore.getRoomPresence(roomId),
    };
  }

  @Post('presence')
  async publishPresence(@Body() dto: PublishPresenceDto, @Req() request: CorrelatedRequest): Promise<Record<string, unknown>> {
    const correlationId = request.correlationId ?? 'missing-correlation-id';

    this.logger.log('Presence update accepted for pub/sub fanout', {
      correlationId,
      userId: dto.userId,
      status: dto.status,
      roomId: dto.roomId,
    });

    const event = await this.presenceService.publishPresence(dto, correlationId);

    return {
      accepted: true,
      reason: 'Presence is ephemeral, so Redis Pub/Sub is used for immediate fanout without durability overhead.',
      event,
    };
  }
}
