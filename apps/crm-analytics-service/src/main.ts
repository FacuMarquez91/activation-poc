
import 'reflect-metadata';
import {
  Controller,
  Injectable,
  Module,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  EventPattern,
  MicroserviceOptions,
  Payload,
  Transport,
} from '@nestjs/microservices';
import mongoose, { Schema } from 'mongoose';

import {
  TOPICS,
  EventEnvelope,
} from '../../../libs/contracts/src';

// MODELO DE AUDITORIA

const auditSchema = new Schema(
  {
    eventId: {
      type: String,
      required: true,
      unique: true,
    },
    eventType: String,
    version: Number,
    occurredAt: String,
    correlationId: String,
    customerId: String,
    source: String,
    topic: String,
    payload: Schema.Types.Mixed,
  },
  {
    timestamps: true,
    versionKey: false,
  },
);

auditSchema.index({ correlationId: 1 });

const AuditEvent = mongoose.model(
  'AuditEvent',
  auditSchema,
);

// SERVICIO CRM

@Injectable()
class CrmAnalyticsService {
  async saveEvent(
    topic: string,
    event: EventEnvelope<Record<string, unknown>>,
  ) {
    if (!event?.eventId || !event.correlationId) {
      console.warn('Evento invalido recibido en CRM');
      return;
    }

    // Un evento se guarda una sola vez,
    // incluso si Kafka vuelve a entregarlo.
    await AuditEvent.updateOne(
      { eventId: event.eventId },
      {
        $setOnInsert: {
          eventId: event.eventId,
          eventType: event.eventType,
          version: event.version,
          occurredAt: event.occurredAt,
          correlationId: event.correlationId,
          customerId: event.customerId,
          source: event.source,
          topic,
          payload: event.payload,
        },
      },
      { upsert: true },
    );

    console.log(
      `CRM: ${event.eventType} | ${event.correlationId}`,
    );
  }
}

// CONSUMIDOR DE LOS CUATRO TOPICS

@Controller()
class CrmAnalyticsController {
  constructor(
    private readonly service: CrmAnalyticsService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async requested(
    @Payload()
    event: EventEnvelope<Record<string, unknown>>,
  ) {
    await this.service.saveEvent(
      TOPICS.ACTIVATION_REQUESTED,
      event,
    );
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async billing(
    @Payload()
    event: EventEnvelope<Record<string, unknown>>,
  ) {
    await this.service.saveEvent(
      TOPICS.BILLING_EVENTS,
      event,
    );
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async provisioning(
    @Payload()
    event: EventEnvelope<Record<string, unknown>>,
  ) {
    await this.service.saveEvent(
      TOPICS.PROVISIONING_EVENTS,
      event,
    );
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async activation(
    @Payload()
    event: EventEnvelope<Record<string, unknown>>,
  ) {
    await this.service.saveEvent(
      TOPICS.ACTIVATION_EVENTS,
      event,
    );
  }
}

@Module({
  controllers: [CrmAnalyticsController],
  providers: [CrmAnalyticsService],
})
class CrmAnalyticsModule {}

async function bootstrap() {
  const mongoUri =
    process.env.MONGO_URI ||
    'mongodb://mongo:27017/crm?replicaSet=rs0';

  await mongoose.connect(mongoUri);

  console.log('CRM conectado a MongoDB');

  const app =
    await NestFactory.createMicroservice<
      MicroserviceOptions
    >(
      CrmAnalyticsModule,
      {
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'crm-analytics-consumer',
            brokers: [
              process.env.KAFKA_BROKERS ||
                'kafka:19092',
            ],
          },
          consumer: {
            groupId: 'crm-analytics-svc',
          },
        },
      },
    );

  await app.listen();

  console.log(
    'CRM Analytics escuchando Kafka',
  );
}

bootstrap().catch((error) => {
  console.error('Error iniciando CRM:', error);
  process.exit(1);
});
