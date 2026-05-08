import { ReportPriority, ReportType } from './reports.dto';

export interface ReportJob {
  jobId: string;
  type: ReportType;
  requestedBy: string;
  dateFrom: string;
  dateTo: string;
  priority: ReportPriority;
  simulateFailure: boolean;
  requestedAt: string;
  correlationId: string;
}
