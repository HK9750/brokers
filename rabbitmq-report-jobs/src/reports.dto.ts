import { IsBoolean, IsEnum, IsISO8601, IsOptional, IsString } from 'class-validator';

export enum ReportType {
  Sales = 'sales',
  Inventory = 'inventory',
  Compliance = 'compliance',
}

export enum ReportPriority {
  Normal = 'normal',
  High = 'high',
  Critical = 'critical',
}

export class GenerateReportDto {
  @IsEnum(ReportType)
  type!: ReportType;

  @IsString()
  requestedBy!: string;

  @IsISO8601({ strict: true })
  dateFrom!: string;

  @IsISO8601({ strict: true })
  dateTo!: string;

  @IsOptional()
  @IsEnum(ReportPriority)
  priority?: ReportPriority;

  @IsOptional()
  @IsBoolean()
  simulateFailure?: boolean;
}
