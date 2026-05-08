import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { AllExceptionsFilter, StructuredLogger } from './common/observability';
import { getKafkaBrokers, getPort } from './config';

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
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: process.env.KAFKA_CONSUMER_CLIENT_ID ?? 'order-risk-projection',
        brokers: getKafkaBrokers(),
      },
      consumer: {
        groupId: process.env.KAFKA_CONSUMER_GROUP ?? 'order-risk-projection',
      },
    },
  });

  await app.startAllMicroservices();

  const port = getPort();
  await app.listen(port);

  logger.log('Kafka order stream service started', {
    port,
    brokers: getKafkaBrokers(),
    consumerGroup: process.env.KAFKA_CONSUMER_GROUP ?? 'order-risk-projection',
  });
}

void bootstrap();
