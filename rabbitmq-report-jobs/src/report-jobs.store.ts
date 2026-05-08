import { Injectable, NotFoundException, OnModuleInit } from '@nestjs/common';
import { StructuredLogger } from './common/observability';
import { PostgresService } from './common/postgres.service';
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
export class ReportJobsStore implements OnModuleInit {
  private readonly logger = new StructuredLogger(ReportJobsStore.name);

  constructor(private readonly postgres: PostgresService) {}

  async onModuleInit(): Promise<void> {
    await this.postgres.query(`
      create table if not exists rabbitmq_report_jobs (
        job_id text primary key,
        status text not null,
        job jsonb not null,
        attempts integer not null default 0,
        artifact text,
        error text,
        queued_at timestamptz not null,
        started_at timestamptz,
        finished_at timestamptz,
        updated_at timestamptz not null
      )
    `);

    this.logger.log('RabbitMQ report job table is ready', { table: 'rabbitmq_report_jobs' });
  }

  async createQueued(job: ReportJob): Promise<ReportJobRecord> {
    const now = new Date().toISOString();
    const result = await this.postgres.query<ReportJobRow>(
      `
        insert into rabbitmq_report_jobs (job_id, status, job, attempts, queued_at, updated_at)
        values ($1, 'queued', $2::jsonb, 0, $3, $3)
        on conflict (job_id) do update set
          status = excluded.status,
          job = excluded.job,
          updated_at = excluded.updated_at
        returning *
      `,
      [job.jobId, JSON.stringify(job), now],
    );
    const record = this.mapRow(result.rows[0]);

    this.logger.log('Report job status recorded as queued', {
      correlationId: job.correlationId,
      jobId: job.jobId,
      type: job.type,
      priority: job.priority,
    });

    return record;
  }

  async markProcessing(job: ReportJob): Promise<ReportJobRecord> {
    await this.ensureExists(job);
    const now = new Date().toISOString();
    const result = await this.postgres.query<ReportJobRow>(
      `
        update rabbitmq_report_jobs
        set status = 'processing', attempts = attempts + 1, started_at = $2, updated_at = $2
        where job_id = $1
        returning *
      `,
      [job.jobId, now],
    );

    return this.mapRow(result.rows[0]);
  }

  async markCompleted(job: ReportJob, artifact: string): Promise<ReportJobRecord> {
    await this.ensureExists(job);
    const now = new Date().toISOString();
    const result = await this.postgres.query<ReportJobRow>(
      `
        update rabbitmq_report_jobs
        set status = 'completed', artifact = $2, error = null, finished_at = $3, updated_at = $3
        where job_id = $1
        returning *
      `,
      [job.jobId, artifact, now],
    );

    return this.mapRow(result.rows[0]);
  }

  async markFailed(job: ReportJob, error: unknown): Promise<ReportJobRecord> {
    await this.ensureExists(job);
    const now = new Date().toISOString();
    const result = await this.postgres.query<ReportJobRow>(
      `
        update rabbitmq_report_jobs
        set status = 'failed', error = $2, finished_at = $3, updated_at = $3
        where job_id = $1
        returning *
      `,
      [job.jobId, error instanceof Error ? error.message : String(error), now],
    );

    return this.mapRow(result.rows[0]);
  }

  async getJob(jobId: string): Promise<ReportJobRecord> {
    const result = await this.postgres.query<ReportJobRow>('select * from rabbitmq_report_jobs where job_id = $1', [jobId]);
    const record = result.rows[0] ? this.mapRow(result.rows[0]) : undefined;

    if (!record) {
      throw new NotFoundException(`Report job ${jobId} was not found in the local status store`);
    }

    return record;
  }

  async listJobs(): Promise<ReportJobRecord[]> {
    const result = await this.postgres.query<ReportJobRow>('select * from rabbitmq_report_jobs order by updated_at desc');
    return result.rows.map((row) => this.mapRow(row));
  }

  private async ensureExists(job: ReportJob): Promise<void> {
    const result = await this.postgres.query('select 1 from rabbitmq_report_jobs where job_id = $1', [job.jobId]);

    if (result.rowCount === 0) {
      await this.createQueued(job);
    }
  }

  private mapRow(row: ReportJobRow): ReportJobRecord {
    return {
      jobId: row.job_id,
      status: row.status,
      job: row.job,
      attempts: row.attempts,
      artifact: row.artifact ?? undefined,
      error: row.error ?? undefined,
      queuedAt: row.queued_at.toISOString(),
      startedAt: row.started_at?.toISOString(),
      finishedAt: row.finished_at?.toISOString(),
      updatedAt: row.updated_at.toISOString(),
    };
  }
}

interface ReportJobRow extends Record<string, unknown> {
  job_id: string;
  status: ReportJobStatus;
  job: ReportJob;
  attempts: number;
  artifact: string | null;
  error: string | null;
  queued_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
  updated_at: Date;
}
