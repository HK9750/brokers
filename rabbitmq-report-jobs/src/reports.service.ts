import { Inject, Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import { lastValueFrom } from 'rxjs';
import { StructuredLogger } from './common/observability';
import { REPORT_PATTERN, REPORT_QUEUE_CLIENT, getReportQueue } from './config';
import { ReportJob } from './report.contract';
import { ReportJobsStore, ReportJobRecord } from './report-jobs.store';
import { GenerateReportDto, ReportPriority } from './reports.dto';

@Injectable()
export class ReportsService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new StructuredLogger(ReportsService.name);

  constructor(
    @Inject(REPORT_QUEUE_CLIENT) private readonly client: ClientProxy,
    private readonly jobsStore: ReportJobsStore,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.client.connect();
    this.logger.log('RabbitMQ producer connected', {
      queue: getReportQueue(),
      pattern: REPORT_PATTERN,
    });
  }

  onModuleDestroy(): void {
    this.client.close();
    this.logger.log('RabbitMQ producer disconnected');
  }

  async enqueueReport(dto: GenerateReportDto, correlationId: string): Promise<ReportJobRecord> {
    const job: ReportJob = {
      jobId: randomUUID(),
      type: dto.type,
      requestedBy: dto.requestedBy,
      dateFrom: dto.dateFrom,
      dateTo: dto.dateTo,
      priority: dto.priority ?? ReportPriority.Normal,
      simulateFailure: dto.simulateFailure ?? false,
      requestedAt: new Date().toISOString(),
      correlationId,
    };
    const record = await this.jobsStore.createQueued(job);

    this.logger.log('Publishing report job to RabbitMQ', {
      correlationId,
      jobId: job.jobId,
      type: job.type,
      priority: job.priority,
      simulateFailure: job.simulateFailure,
      queue: getReportQueue(),
      pattern: REPORT_PATTERN,
    });

    await lastValueFrom(this.client.emit(REPORT_PATTERN, job));

    this.logger.log('Report job enqueued', {
      correlationId,
      jobId: job.jobId,
      queue: getReportQueue(),
    });

    return record;
  }

  getReportJob(jobId: string): Promise<ReportJobRecord> {
    return this.jobsStore.getJob(jobId);
  }

  listReportJobs(): Promise<ReportJobRecord[]> {
    return this.jobsStore.listJobs();
  }
}
