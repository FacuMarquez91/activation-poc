import 'reflect-metadata';
import { randomUUID } from 'crypto';
import {
  Controller,
  Inject,
  Injectable,
  Module,
  OnModuleInit,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  ClientKafka,
  ClientsModule,
  EventPattern,
  MicroserviceOptions,
  Payload,
  Transport,
} from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import mongoose, { Schema } from 'mongoose';

import {
  TOPICS,
  EVENT_TYPES,
  ActivationRequestedEvent,
  EventEnvelope,
} from '../../../libs/contracts/src';
import { withRetry } from '../../../libs/kafka-toolkit/src';

const provisioningSchema = new Schema(
  {
    activationId: {
      type: String,
      required: true,
      unique: true,
    },
    customerId: String,
    planId: String,
    status: String,
    reason: String,
  },
  { timestamps: true, versionKey: false },
);

const ProvisioningOrder = mongoose.model(
  'ProvisioningOrder',
  provisioningSchema,
);

@Injectable()
class ProvisioningService implements OnModuleInit {
  constructor(
    @Inject('KAFKA_PRODUCER')
    private readonly kafka: ClientKafka,
  ) { }

  async onModuleInit() {
    await this.kafka.connect();
  }

  async processActivation(
    event: ActivationRequestedEvent,
  ) {
    if (
      event.eventType !==
      EVENT_TYPES.ACTIVATION_REQUESTED
    ) {
      return;
    }

    const activationId = event.correlationId;

    console.log(
      `Iniciando aprovisionamiento: ${activationId}`,
    );

    // Simular un procesamiento de 1 a 3 segundos.
    const delay = 1000 + Math.floor(Math.random() * 2001);

    await new Promise<void>((resolve) =>
      setTimeout(resolve, delay),
    );

    const shouldFail =
      event.payload.simulateFailure === 'provisioning';

    const eventType = shouldFail
      ? EVENT_TYPES.PROVISIONING_FAILED
      : EVENT_TYPES.PROVISIONING_COMPLETED;

    const status = shouldFail ? 'FAILED' : 'COMPLETED';

    await ProvisioningOrder.updateOne(
      { activationId },
      {
        $setOnInsert: {
          activationId,
          customerId: event.customerId,
          planId: event.payload.planId,
          status,
          ...(shouldFail
            ? { reason: 'Fallo simulado de aprovisionamiento' }
            : {}),
        },
      },
      { upsert: true },
    );

    const result: EventEnvelope<Record<string, string>> = {
      eventId: randomUUID(),
      eventType,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: activationId,
      customerId: event.customerId,
      source: 'provisioning-service',
      payload: {
        activationId,
        status,
        ...(shouldFail
          ? { reason: 'Fallo simulado de aprovisionamiento' }
          : {}),
      },
    };

    await firstValueFrom(
      this.kafka.emit(TOPICS.PROVISIONING_EVENTS, {
        key: event.customerId,
        value: result,
      }),
    );

    console.log(`Evento publicado: ${eventType}`);
    console.log(
      `Aprovisionamiento finalizado: ${activationId}`,
    );
  }
  async publishToDlq(
    event: ActivationRequestedEvent,
    error: unknown,
  ) {
    const errorMessage =
      error instanceof Error
        ? error.message
        : String(error);

    const dlqMessage = {
      originalEvent: event,
      originalTopic: TOPICS.ACTIVATION_REQUESTED,
      failedAt: new Date().toISOString(),
      error: errorMessage,
      failedService: 'provisioning-service',
    };

    await firstValueFrom(
      this.kafka.emit(
        TOPICS.ACTIVATION_REQUESTED_DLQ,
        {
          key: event.customerId,
          value: dlqMessage,
        },
      ),
    );

    console.error(
      `Evento enviado a DLQ: ${event.correlationId}`,
    );
  }
}

@Controller()
class ProvisioningController {
  constructor(
    private readonly service: ProvisioningService,
  ) { }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivation(
    @Payload() event: ActivationRequestedEvent,
  ) {
    try {
      await withRetry(
        () => this.service.processActivation(event),
        {
          onRetry: (error, retryNumber, delayMs) => {
            console.warn(
              `Provisioning: reintento ${retryNumber}/3 ` +
              `en ${delayMs} ms`,
              error,
            );
          },
        },
      );
    } catch (error) {
      console.error(
        `Provisioning: reintentos agotados para ` +
        `${event.correlationId}`,
        error,
      );

      // Si falla la publicación en DLQ,
      // dejamos que el error se propague.
      await this.service.publishToDlq(event, error);
    }
  }
}

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_PRODUCER',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'provisioning-producer',
            brokers: [
              process.env.KAFKA_BROKERS || 'kafka:19092',
            ],
          },
          consumer: {
            groupId: 'provisioning-producer',
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [ProvisioningController],
  providers: [ProvisioningService],
})
class ProvisioningModule { }

async function bootstrap() {
  const mongoUri =
    process.env.MONGO_URI ||
    'mongodb://mongo:27017/provisioning?replicaSet=rs0';

  await mongoose.connect(mongoUri);

  console.log('Provisioning conectado a MongoDB');

  const app =
    await NestFactory.createMicroservice<MicroserviceOptions>(
      ProvisioningModule,
      {
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'provisioning-consumer',
            brokers: [
              process.env.KAFKA_BROKERS || 'kafka:19092',
            ],
          },
          consumer: {
            groupId: 'provisioning-svc',
          },
        },
      },
    );

  await app.listen();

  console.log('Provisioning-service escuchando Kafka');
}

bootstrap().catch((error) => {
  console.error('Error iniciando provisioning-service:', error);
  process.exit(1);
});
