import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { AllExceptionsFilter, StructuredLogger } from './common/observability';
import { getNatsQueueGroup, getNatsServers, getPort } from './config';

async function bootstrap(): Promise<void> {
  const logger = new StructuredLogger('Bootstrap');

  process.on('unhandledRejection', (reason) => {
    logger.error('Unhandled promise rejection', { reason });
  });

  process.on('uncaughtException', (error) => {
    logger.error('Uncaught exception', error.stack, { errorName: error.name, errorMessage: error.message });
  });

  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  app.useLogger(logger);
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );
  app.useGlobalFilters(new AllExceptionsFilter(logger));
  app.enableShutdownHooks();

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.NATS,
    options: {
      servers: getNatsServers(),
      queue: getNatsQueueGroup(),
    },
  });

  await app.startAllMicroservices();

  const port = getPort();
  await app.listen(port);

  logger.log('NATS device control service started', {
    port,
    servers: getNatsServers(),
    queueGroup: getNatsQueueGroup(),
  });
}

void bootstrap();
