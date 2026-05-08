import { Injectable, NotFoundException } from '@nestjs/common';
import { StructuredLogger } from './common/observability';
import { ReportJob } from './report.contract';

export type ReportJobStatus = 'queued' | 'processing' | 'completed' | 'failed';

export interface ReportJobRecord {
  jobId: string;
  status: ReportJobStatus;
  job: ReportJob;
  attempts: number;
  artifact?: string;
  error?: string;
  queuedAt: string;
  startedAt?: string;
  finishedAt?: string;
  updatedAt: string;
}

@Injectable()
export class ReportJobsStore {
  private readonly logger = new StructuredLogger(ReportJobsStore.name);
  private readonly jobs = new Map<string, ReportJobRecord>();

  createQueued(job: ReportJob): ReportJobRecord {
    const now = new Date().toISOString();
    const record: ReportJobRecord = {
      jobId: job.jobId,
      status: 'queued',
      job,
      attempts: 0,
      queuedAt: now,
      updatedAt: now,
    };

    this.jobs.set(job.jobId, record);
    this.logger.log('Report job status recorded as queued', {
      correlationId: job.correlationId,
      jobId: job.jobId,
      type: job.type,
      priority: job.priority,
    });

    return record;
  }

  markProcessing(job: ReportJob): ReportJobRecord {
    const record = this.getExistingOrCreate(job);
    const now = new Date().toISOString();
    const updated: ReportJobRecord = {
      ...record,
      status: 'processing',
      attempts: record.attempts + 1,
      startedAt: now,
      updatedAt: now,
    };

    this.jobs.set(job.jobId, updated);
    return updated;
  }

  markCompleted(job: ReportJob, artifact: string): ReportJobRecord {
    const record = this.getExistingOrCreate(job);
    const now = new Date().toISOString();
    const updated: ReportJobRecord = {
      ...record,
      status: 'completed',
      artifact,
      finishedAt: now,
      updatedAt: now,
    };

    this.jobs.set(job.jobId, updated);
    return updated;
  }

  markFailed(job: ReportJob, error: unknown): ReportJobRecord {
    const record = this.getExistingOrCreate(job);
    const now = new Date().toISOString();
    const updated: ReportJobRecord = {
      ...record,
      status: 'failed',
      error: error instanceof Error ? error.message : String(error),
      finishedAt: now,
      updatedAt: now,
    };

    this.jobs.set(job.jobId, updated);
    return updated;
  }

  getJob(jobId: string): ReportJobRecord {
    const record = this.jobs.get(jobId);

    if (!record) {
      throw new NotFoundException(`Report job ${jobId} was not found in the local status store`);
    }

    return record;
  }

  listJobs(): ReportJobRecord[] {
    return [...this.jobs.values()].sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
  }

  private getExistingOrCreate(job: ReportJob): ReportJobRecord {
    return this.jobs.get(job.jobId) ?? this.createQueued(job);
  }
}
