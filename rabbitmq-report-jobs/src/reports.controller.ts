import { Body, Controller, Get, Param, Post, Req } from '@nestjs/common';
import { CorrelatedRequest, StructuredLogger } from './common/observability';
import { getReportQueue } from './config';
import { GenerateReportDto } from './reports.dto';
import { ReportsService } from './reports.service';

@Controller()
export class ReportsController {
  private readonly logger = new StructuredLogger(ReportsController.name);

  constructor(private readonly reportsService: ReportsService) {}

  @Get('health')
  health(): Record<string, string> {
    return { status: 'ok', broker: 'rabbitmq', queue: getReportQueue() };
  }

  @Post('reports')
  async generateReport(@Body() dto: GenerateReportDto, @Req() request: CorrelatedRequest): Promise<Record<string, unknown>> {
    const correlationId = request.correlationId ?? 'missing-correlation-id';

    this.logger.log('Report request accepted for background processing', {
      correlationId,
      type: dto.type,
      requestedBy: dto.requestedBy,
      priority: dto.priority ?? 'normal',
    });

    const job = await this.reportsService.enqueueReport(dto, correlationId);

    return {
      accepted: true,
      reason: 'Report generation is queued because it is slow and should not block the HTTP request.',
      queue: getReportQueue(),
      statusUrl: `/reports/${job.jobId}`,
      job,
    };
  }

  @Get('reports')
  listReportJobs(): Record<string, unknown> {
    const jobs = this.reportsService.listReportJobs();

    return {
      count: jobs.length,
      jobs,
    };
  }

  @Get('reports/:jobId')
  getReportJob(@Param('jobId') jobId: string): Record<string, unknown> {
    return {
      job: this.reportsService.getReportJob(jobId),
    };
  }
}
