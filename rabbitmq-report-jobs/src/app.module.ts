import { MiddlewareConsumer, Module, NestModule } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { HttpLoggingMiddleware } from './common/observability';
import { REPORT_QUEUE_CLIENT, getRabbitUrl, getReportQueue } from './config';
import { ReportJobsStore } from './report-jobs.store';
import { ReportsController } from './reports.controller';
import { ReportsService } from './reports.service';
import { ReportsWorker } from './reports.worker';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: REPORT_QUEUE_CLIENT,
        transport: Transport.RMQ,
        options: {
          urls: [getRabbitUrl()],
          queue: getReportQueue(),
          queueOptions: { durable: true },
          persistent: true,
        },
      },
    ]),
  ],
  controllers: [ReportsController, ReportsWorker],
  providers: [ReportsService, ReportJobsStore],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(HttpLoggingMiddleware).forRoutes('*');
  }
}
