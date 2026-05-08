import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Injectable,
  LoggerService,
  NestMiddleware,
} from '@nestjs/common';
import { randomUUID } from 'crypto';
import { NextFunction, Request, Response } from 'express';

type Metadata = Record<string, unknown>;
type Level = 'info' | 'error' | 'warn' | 'debug' | 'verbose';

export type CorrelatedRequest = Request & { correlationId?: string };

export class StructuredLogger implements LoggerService {
  constructor(private readonly context = 'Application') {}

  log(message: unknown, ...optionalParams: unknown[]): void {
    this.write('info', message, optionalParams);
  }

  error(message: unknown, ...optionalParams: unknown[]): void {
    this.write('error', message, optionalParams, true);
  }

  warn(message: unknown, ...optionalParams: unknown[]): void {
    this.write('warn', message, optionalParams);
  }

  debug(message: unknown, ...optionalParams: unknown[]): void {
    this.write('debug', message, optionalParams);
  }

  verbose(message: unknown, ...optionalParams: unknown[]): void {
    this.write('verbose', message, optionalParams);
  }

  private write(level: Level, message: unknown, optionalParams: unknown[], isError = false): void {
    const { context, trace, meta } = this.parseParams(optionalParams, isError);
    const payload: Metadata = {
      timestamp: new Date().toISOString(),
      level,
      context,
      message: message instanceof Error ? message.message : message,
      ...meta,
    };

    if (message instanceof Error) {
      payload.errorName = message.name;
      payload.stack = message.stack;
    }

    if (trace) {
      payload.trace = trace;
    }

    const target = level === 'error' || level === 'warn' ? process.stderr : process.stdout;
    target.write(`${this.stringify(payload)}\n`);
  }

  private parseParams(optionalParams: unknown[], isError: boolean): { context: string; trace?: string; meta: Metadata } {
    const params = [...optionalParams];
    const meta: Metadata = {};
    let context = this.context;
    let trace: string | undefined;

    if (isError && typeof params[0] === 'string') {
      trace = params.shift() as string;
    }

    for (const param of params) {
      if (typeof param === 'string') {
        context = param;
        continue;
      }

      if (param && typeof param === 'object' && !Array.isArray(param)) {
        Object.assign(meta, param as Metadata);
        continue;
      }

      if (param !== undefined) {
        meta.extra = param;
      }
    }

    return { context, trace, meta };
  }

  private stringify(payload: Metadata): string {
    const seen = new WeakSet<object>();

    return JSON.stringify(payload, (_key, value: unknown) => {
      if (typeof value === 'bigint') {
        return value.toString();
      }

      if (value instanceof Error) {
        return { name: value.name, message: value.message, stack: value.stack };
      }

      if (value && typeof value === 'object') {
        if (seen.has(value)) {
          return '[Circular]';
        }
        seen.add(value);
      }

      return value;
    });
  }
}

@Injectable()
export class HttpLoggingMiddleware implements NestMiddleware {
  private readonly logger = new StructuredLogger('Http');

  use(request: CorrelatedRequest, response: Response, next: NextFunction): void {
    const headerCorrelationId = request.headers['x-correlation-id'];
    const correlationId = Array.isArray(headerCorrelationId) ? headerCorrelationId[0] : headerCorrelationId || randomUUID();
    const startedAt = process.hrtime.bigint();

    request.correlationId = correlationId;
    response.setHeader('x-correlation-id', correlationId);

    this.logger.log('HTTP request started', {
      correlationId,
      method: request.method,
      path: request.originalUrl,
      ip: request.ip,
      userAgent: request.headers['user-agent'],
    });

    response.on('finish', () => {
      const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
      const metadata = {
        correlationId,
        method: request.method,
        path: request.originalUrl,
        statusCode: response.statusCode,
        durationMs: Number(durationMs.toFixed(2)),
      };

      if (response.statusCode >= 500) {
        this.logger.error('HTTP request completed with server error', metadata);
      } else if (response.statusCode >= 400) {
        this.logger.warn('HTTP request completed with client error', metadata);
      } else {
        this.logger.log('HTTP request completed', metadata);
      }
    });

    next();
  }
}

@Catch()
export class AllExceptionsFilter implements ExceptionFilter {
  constructor(private readonly logger = new StructuredLogger('ExceptionFilter')) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    if (host.getType() !== 'http') {
      this.logger.error('Unhandled non-HTTP exception', exception instanceof Error ? exception.stack : undefined, {
        error: exception,
        contextType: host.getType(),
      });
      return;
    }

    const context = host.switchToHttp();
    const response = context.getResponse<Response>();
    const request = context.getRequest<CorrelatedRequest>();
    const statusCode = exception instanceof HttpException ? exception.getStatus() : HttpStatus.INTERNAL_SERVER_ERROR;
    const exceptionResponse = exception instanceof HttpException ? exception.getResponse() : 'Internal server error';
    const errorMessage = this.extractMessage(exceptionResponse);

    this.logger.error('HTTP exception captured', exception instanceof Error ? exception.stack : undefined, {
      correlationId: request.correlationId,
      method: request.method,
      path: request.originalUrl,
      statusCode,
      error: exceptionResponse,
    });

    response.status(statusCode).json({
      statusCode,
      timestamp: new Date().toISOString(),
      path: request.originalUrl,
      correlationId: request.correlationId,
      error: errorMessage,
    });
  }

  private extractMessage(exceptionResponse: string | object): unknown {
    if (typeof exceptionResponse === 'string') {
      return exceptionResponse;
    }

    if ('message' in exceptionResponse) {
      return (exceptionResponse as { message: unknown }).message;
    }

    return exceptionResponse;
  }
}
