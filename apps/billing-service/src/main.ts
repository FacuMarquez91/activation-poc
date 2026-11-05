
import 'reflect-metadata';
import { randomUUID } from 'crypto';
import {
  Controller,
  Injectable,
  Inject,
  Module,
  OnModuleInit,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  ClientKafka,
  ClientsModule,
  EventPattern,
  Payload,
  Transport,
  MicroserviceOptions,
} from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import mongoose, { Schema } from 'mongoose';

import {
  TOPICS,
  EVENT_TYPES,
  ActivationRequestedEvent,
  EventEnvelope,
} from '../../../libs/contracts/src';

// MODELO DE CUENTA DE FACTURACION

const billingSchema = new Schema(
  {
    activationId: {
      type: String,
      required: true,
      unique: true,
    },
    customerId: String,
    planId: String,
    status: {
      type: String,
      default: 'CREATED',
    },
  },
  { timestamps: true, versionKey: false },
);

const BillingAccount = mongoose.model(
  'BillingAccount',
  billingSchema,
);

// SERVICIO DE FACTURACION

@Injectable()
class BillingService implements OnModuleInit {
  constructor(
    @Inject('KAFKA_PRODUCER')
    private readonly kafka: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafka.connect();
  }

  // PROCESAR SOLICITUD DE ACTIVACION

  async processActivation(
    event: ActivationRequestedEvent,
  ) {
    if (
      event?.eventType !==
      EVENT_TYPES.ACTIVATION_REQUESTED
    ) {
      return;
    }

    const activationId = event.correlationId;

    console.log(
      `Procesando facturacion: ${activationId}`,
    );

    // Fallo de negocio simulado.
    if (event.payload.simulateFailure === 'billing') {
      await this.publishResult(
        event,
        EVENT_TYPES.BILLING_FAILED,
        { reason: 'Fallo simulado de facturacion' },
      );
      return;
    }

    // Si la cuenta ya fue cancelada, no reactivarla.
    // setOnInsert evita sobrescribir una compensacion.
    await BillingAccount.updateOne(
      { activationId },
      {
        $setOnInsert: {
          activationId,
          customerId: event.customerId,
          planId: event.payload.planId,
          status: 'CREATED',
        },
      },
      { upsert: true },
    );

    const account = await BillingAccount.findOne({
      activationId,
    });

    if (account?.status === 'CANCELLED') {
      console.log(
        `Cuenta ya cancelada: ${activationId}`,
      );
      return;
    }

    await this.publishResult(
      event,
      EVENT_TYPES.BILLING_ACCOUNT_CREATED,
      { activationId },
    );

    console.log(
      `Facturacion procesada: ${activationId}`,
    );
  }

  // COMPENSAR UNA ACTIVACION FALLIDA

  async compensate(
    event: EventEnvelope<Record<string, string>>,
  ) {
    if (
      event?.eventType !==
      EVENT_TYPES.ACTIVATION_FAILED
    ) {
      return;
    }

    const activationId = event.correlationId;

    console.log(
      `Compensando activacion: ${activationId}`,
    );

    // Si la cuenta existe, se cancela.
    // Si todavia no existe, se registra CANCELLED
    // para impedir que se active posteriormente.
    await BillingAccount.updateOne(
      { activationId },
      {
        $set: {
          status: 'CANCELLED',
        },
        $setOnInsert: {
          activationId,
          customerId: event.customerId,
        },
      },
      { upsert: true },
    );

    const compensationEvent: EventEnvelope<
      Record<string, string>
    > = {
      eventId: randomUUID(),
      eventType:
        EVENT_TYPES.BILLING_ACCOUNT_CANCELLED,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: activationId,
      customerId: event.customerId,
      source: 'billing-service',
      payload: {
        activationId,
        status: 'CANCELLED',
      },
    };

    await firstValueFrom(
      this.kafka.emit(TOPICS.BILLING_EVENTS, {
        key: event.customerId,
        value: compensationEvent,
      }),
    );

    console.log(
      `Evento publicado: BillingAccountCancelled`,
    );
    console.log(
      `Compensacion finalizada: ${activationId}`,
    );
  }

  // PUBLICAR RESULTADOS EN KAFKA

  private async publishResult(
    original: ActivationRequestedEvent,
    eventType: string,
    payload: Record<string, string>,
  ) {
    const result: EventEnvelope<
      Record<string, string>
    > = {
      eventId: randomUUID(),
      eventType,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: original.correlationId,
      customerId: original.customerId,
      source: 'billing-service',
      payload,
    };

    await firstValueFrom(
      this.kafka.emit(TOPICS.BILLING_EVENTS, {
        key: original.customerId,
        value: result,
      }),
    );

    console.log(`Evento publicado: ${eventType}`);
  }
}

// CONSUMIDORES KAFKA

@Controller()
class BillingController {
  constructor(
    private readonly service: BillingService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivation(
    @Payload() event: ActivationRequestedEvent,
  ) {
    await this.service.processActivation(event);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationResult(
    @Payload()
    event: EventEnvelope<Record<string, string>>,
  ) {
    await this.service.compensate(event);
  }
}

// MODULO PRINCIPAL

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_PRODUCER',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'billing-producer',
            brokers: [
              process.env.KAFKA_BROKERS ||
                'kafka:19092',
            ],
          },
          consumer: {
            groupId: 'billing-producer',
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [BillingController],
  providers: [BillingService],
})
class BillingModule {}

// INICIAR MICROSERVICIO

async function bootstrap() {
  const mongoUri =
    process.env.MONGO_URI ||
    'mongodb://mongo:27017/billing?replicaSet=rs0';

  await mongoose.connect(mongoUri);

  console.log('Billing conectado a MongoDB');

  const app =
    await NestFactory.createMicroservice<
      MicroserviceOptions
    >(
      BillingModule,
      {
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'billing-consumer',
            brokers: [
              process.env.KAFKA_BROKERS ||
                'kafka:19092',
            ],
          },
          consumer: {
            groupId: 'billing-svc',
          },
        },
      },
    );

  await app.listen();

  console.log(
    'Billing-service escuchando Kafka',
  );
}

bootstrap().catch((error) => {
  console.error(
    'Error iniciando billing-service:',
    error,
  );
  process.exit(1);
});
