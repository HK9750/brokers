import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, Payload, RmqContext } from '@nestjs/microservices';
import { StructuredLogger } from './common/observability';
import { REPORT_PATTERN, getReportQueue, getReportWorkMs } from './config';
import { ReportJob } from './report.contract';
import { ReportJobsStore } from './report-jobs.store';
import { ReportPriority } from './reports.dto';

@Controller()
export class ReportsWorker {
  private readonly logger = new StructuredLogger(ReportsWorker.name);

  constructor(private readonly jobsStore: ReportJobsStore) {}

  @EventPattern(REPORT_PATTERN)
  async handleReportJob(@Payload() job: ReportJob, @Ctx() context: RmqContext): Promise<void> {
    const channel = context.getChannelRef();
    const message = context.getMessage();
    const startedAt = process.hrtime.bigint();

    this.logger.log('RabbitMQ report job received', {
      correlationId: job.correlationId,
      jobId: job.jobId,
      type: job.type,
      priority: job.priority,
      queue: getReportQueue(),
      deliveryTag: message.fields.deliveryTag,
      redelivered: message.fields.redelivered,
    });
    this.jobsStore.markProcessing(job);

    try {
      const artifact = await this.generateReport(job);
      const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
      const record = this.jobsStore.markCompleted(job, artifact);

      channel.ack(message);

      this.logger.log('RabbitMQ report job acknowledged', {
        correlationId: job.correlationId,
        jobId: job.jobId,
        durationMs: Number(durationMs.toFixed(2)),
        queue: getReportQueue(),
        status: record.status,
        artifact: record.artifact,
      });
    } catch (error) {
      const record = this.jobsStore.markFailed(job, error);
      channel.nack(message, false, false);
      this.logger.error('RabbitMQ report job failed and was nacked', error instanceof Error ? error.stack : undefined, {
        correlationId: job.correlationId,
        jobId: job.jobId,
        error,
        requeued: false,
        status: record.status,
      });
    }
  }

  private async generateReport(job: ReportJob): Promise<string> {
    if (job.priority === ReportPriority.Critical) {
      this.logger.warn('Critical report job is consuming the reserved worker slot', {
        correlationId: job.correlationId,
        jobId: job.jobId,
        type: job.type,
      });
    }

    await new Promise((resolve) => setTimeout(resolve, getReportWorkMs()));

    if (job.simulateFailure) {
      throw new Error('Simulated report renderer failure');
    }

    const artifact = `reports/${job.type}/${job.jobId}.pdf`;

    this.logger.log('Report artifact generated', {
      correlationId: job.correlationId,
      jobId: job.jobId,
      artifact,
      requestedBy: job.requestedBy,
    });

    return artifact;
  }
}
