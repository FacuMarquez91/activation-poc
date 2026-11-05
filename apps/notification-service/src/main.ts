
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
import * as nodemailer from 'nodemailer';

import {
  TOPICS,
  EVENT_TYPES,
  EventEnvelope,
} from '../../../libs/contracts/src';

// MODELO MONGODB

const notificationSchema = new Schema(
  {
    activationId: {
      type: String,
      required: true,
    },
    customerId: String,
    eventType: {
      type: String,
      required: true,
    },
    eventId: String,
    status: String,
    subject: String,
  },
  {
    timestamps: true,
    versionKey: false,
  },
);

notificationSchema.index(
  { activationId: 1, eventType: 1 },
  { unique: true },
);

const Notification = mongoose.model(
  'Notification',
  notificationSchema,
);

// SERVICIO DE NOTIFICACIONES

@Injectable()
class NotificationService {
  private readonly transporter =
    nodemailer.createTransport({
      host: process.env.SMTP_HOST || 'mailhog',
      port: Number(process.env.SMTP_PORT || 1025),
      secure: false,
    });

  async processEvent(
    event: EventEnvelope<Record<string, string>>,
  ) {
    if (
      event?.eventType !==
        EVENT_TYPES.ACTIVATION_COMPLETED &&
      event?.eventType !==
        EVENT_TYPES.ACTIVATION_FAILED
    ) {
      return;
    }

    const activationId = event.correlationId;

    const previous = await Notification.findOne({
      activationId,
      eventType: event.eventType,
      status: 'SENT',
    });

    if (previous) {
      console.log(
        `Notificacion ya enviada: ${activationId}`,
      );
      return;
    }

    const successful =
      event.eventType ===
      EVENT_TYPES.ACTIVATION_COMPLETED;

    const subject = successful
      ? 'Tu plan fue activado correctamente'
      : 'No pudimos activar tu plan';

    const message = successful
      ? `Hola ${event.customerId}.

Tu plan fue activado correctamente.

Identificador: ${activationId}

Gracias por elegirnos.`
      : `Hola ${event.customerId}.

No fue posible completar la activacion.

Identificador: ${activationId}

Por favor, contacta con soporte.`;

    // Destinatario simulado para la POC.
    const to = `${event.customerId}@demo.local`;

    await this.transporter.sendMail({
      from: 'activaciones@demo.local',
      to,
      subject,
      text: message,
    });

    await Notification.updateOne(
      {
        activationId,
        eventType: event.eventType,
      },
      {
        $set: {
          customerId: event.customerId,
          eventId: event.eventId,
          status: 'SENT',
          subject,
        },
      },
      { upsert: true },
    );

    console.log(
      `Email enviado a ${to}: ${subject}`,
    );
  }
}

// CONSUMIDOR KAFKA

@Controller()
class NotificationController {
  constructor(
    private readonly service: NotificationService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivation(
    @Payload()
    event: EventEnvelope<Record<string, string>>,
  ) {
    await this.service.processEvent(event);
  }
}

// MODULO PRINCIPAL

@Module({
  controllers: [NotificationController],
  providers: [NotificationService],
})
class NotificationModule {}

// INICIAR MICROSERVICIO

async function bootstrap() {
  const mongoUri =
    process.env.MONGO_URI ||
    'mongodb://mongo:27017/notification?replicaSet=rs0';

  await mongoose.connect(mongoUri);

  console.log('Notification conectado a MongoDB');

  const app =
    await NestFactory.createMicroservice<
      MicroserviceOptions
    >(
      NotificationModule,
      {
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'notification-consumer',
            brokers: [
              process.env.KAFKA_BROKERS ||
                'kafka:19092',
            ],
          },
          consumer: {
            groupId: 'notification-svc',
          },
        },
      },
    );

  await app.listen();

  console.log(
    'Notification-service escuchando Kafka',
  );
}

bootstrap().catch((error) => {
  console.error(
    'Error iniciando notification-service:',
    error,
  );
  process.exit(1);
});
