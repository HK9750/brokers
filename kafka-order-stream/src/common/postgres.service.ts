import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool, QueryResult, QueryResultRow } from 'pg';
import { getPostgresConfig } from '../config';
import { StructuredLogger } from './observability';

@Injectable()
export class PostgresService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new StructuredLogger(PostgresService.name);
  private readonly config = getPostgresConfig();
  private readonly pool = new Pool(this.config);

  async onModuleInit(): Promise<void> {
    await this.query('select 1');
    this.logger.log('Postgres connection established', {
      host: this.config.host,
      port: this.config.port,
      database: this.config.database,
      user: this.config.user,
      poolMax: this.config.max,
    });
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
    this.logger.log('Postgres connection pool closed');
  }

  async query<T extends QueryResultRow = QueryResultRow>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    const startedAt = process.hrtime.bigint();

    try {
      const result = await this.pool.query<T>(sql, params);
      const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;

      this.logger.debug('Postgres query completed', {
        durationMs: Number(durationMs.toFixed(2)),
        rowCount: result.rowCount,
      });

      return result;
    } catch (error) {
      this.logger.error('Postgres query failed', error instanceof Error ? error.stack : undefined, { error });
      throw error;
    }
  }
}
