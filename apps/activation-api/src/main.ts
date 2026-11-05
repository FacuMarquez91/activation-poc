
import 'reflect-metadata';
import { randomUUID } from 'crypto';
import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpException,
  HttpStatus,
  Inject,
  Injectable,
  Module,
  OnModuleInit,
  Param,
  Post,
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
  FailureMode,
  ActivationRequestedEvent,
} from '../../../libs/contracts/src';

// ==========================================
// MODELO DE DATOS - MONGODB
// ==========================================

const activationSchema = new Schema(
  {
    _id: String,
    customerId: { type: String, required: true },
    planId: { type: String, required: true },
    status: { type: String, default: 'PENDING' },
    simulateFailure: { type: String, default: 'none' },
    steps: { type: Schema.Types.Mixed, default: {} },
    history: {
      type: [Schema.Types.Mixed],
      default: [],
    },
  },
  {
    timestamps: true,
    versionKey: false,
  },
);

activationSchema.index({ customerId: 1 });

const Activation = mongoose.model(
  'Activation',
  activationSchema,
);

// ==========================================
// SERVICIO DE ACTIVACIONES
// ==========================================

@Injectable()
class ActivationService implements OnModuleInit {
  constructor(
    @Inject('KAFKA_PRODUCER')
    private readonly kafka: ClientKafka,
  ) {}

  async onModuleInit() {
    await this.kafka.connect();
  }

  // ========================================
  // CREAR UNA ACTIVACION
  // ========================================

  async create(data: {
    customerId: string;
    planId: string;
    simulateFailure?: FailureMode;
  }) {
    if (
      typeof data?.customerId !== 'string' ||
      !data.customerId.trim() ||
      typeof data?.planId !== 'string' ||
      !data.planId.trim()
    ) {
      throw new HttpException(
        'customerId y planId son obligatorios',
        HttpStatus.BAD_REQUEST,
      );
    }

    const simulateFailure =
      data.simulateFailure ?? 'none';

    if (
      !['none', 'billing', 'provisioning'].includes(
        simulateFailure,
      )
    ) {
      throw new HttpException(
        'simulateFailure debe ser none, billing o provisioning',
        HttpStatus.BAD_REQUEST,
      );
    }

    const activationId = randomUUID();
    const customerId = data.customerId.trim();
    const planId = data.planId.trim();

    const event: ActivationRequestedEvent = {
      eventId: randomUUID(),
      eventType: EVENT_TYPES.ACTIVATION_REQUESTED,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: activationId,
      customerId,
      source: 'activation-api',
      payload: {
        planId,
        channel: 'web',
        simulateFailure,
      },
    };

    await Activation.create({
      _id: activationId,
      customerId,
      planId,
      status: 'PENDING',
      simulateFailure,
      steps: {},
      history: [
        {
          eventType: event.eventType,
          at: event.occurredAt,
          eventId: event.eventId,
        },
      ],
    });

    await firstValueFrom(
      this.kafka.emit(
        TOPICS.ACTIVATION_REQUESTED,
        {
          key: customerId,
          value: event,
        },
      ),
    );

    console.log(
      `Activacion solicitada: ${activationId}`,
    );

    return {
      activationId,
      status: 'PENDING',
    };
  }

  // ========================================
  // AGREGADOR DE LA SAGA
  // ========================================

  async processResult(event: {
    eventId: string;
    eventType: string;
    correlationId: string;
    customerId: string;
    occurredAt: string;
  }) {
    if (
      !event ||
      !event.eventId ||
      !event.correlationId ||
      !event.customerId
    ) {
      console.warn('Evento de resultado invalido');
      return;
    }

    const billingTypes: string[] = [
      EVENT_TYPES.BILLING_ACCOUNT_CREATED,
      EVENT_TYPES.BILLING_FAILED,
    ];

    const provisioningTypes: string[] = [
      EVENT_TYPES.PROVISIONING_COMPLETED,
      EVENT_TYPES.PROVISIONING_FAILED,
    ];

    const isBilling = billingTypes.includes(
      event.eventType,
    );

    const isProvisioning = provisioningTypes.includes(
      event.eventType,
    );

    if (!isBilling && !isProvisioning) {
      return;
    }

    const activationId = event.correlationId;

    const activation = await Activation.findById(
      activationId,
    );

    if (!activation) {
      console.warn(
        `Activacion no encontrada: ${activationId}`,
      );
      return;
    }

    if (
      activation.get('customerId') !==
      event.customerId
    ) {
      console.warn(
        `Cliente incorrecto para ${activationId}`,
      );
      return;
    }

    const step = isBilling
      ? 'billing'
      : 'provisioning';

    const failed =
  event.eventType === EVENT_TYPES.BILLING_FAILED ||
  event.eventType === EVENT_TYPES.PROVISIONING_FAILED;

    const historyEntry = {
      eventType: event.eventType,
      eventId: event.eventId,
      at: event.occurredAt,
    };

    // Evitar registrar dos veces el mismo evento.
    const previousHistory = activation.get(
      'history',
    ) as Array<{ eventId?: string }>;

    if (
      previousHistory?.some(
        (item) => item.eventId === event.eventId,
      )
    ) {
      console.log(
        `Evento duplicado ignorado: ${event.eventId}`,
      );
      return;
    }

    // Una saga finalizada no vuelve a cambiar.
    // Registramos los resultados tardios.
    if (
      ['ACTIVE', 'FAILED'].includes(
        activation.get('status'),
      )
    ) {
      await Activation.updateOne(
        {
          _id: activationId,
          'history.eventId': {
            $ne: event.eventId,
          },
        },
        {
          $push: {
            history: historyEntry,
          },
        },
      );

      return;
    }

    // Guardar resultado parcial del servicio.
    const updated = await Activation.findOneAndUpdate(
      {
        _id: activationId,
        status: {
          $nin: ['ACTIVE', 'FAILED'],
        },
        'history.eventId': {
          $ne: event.eventId,
        },
      },
      {
        $set: {
          [`steps.${step}`]: {
            status: failed ? 'FAILED' : 'OK',
            at: event.occurredAt,
          },
          status: 'IN_PROGRESS',
        },
        $push: {
          history: historyEntry,
        },
      },
      {
        new: true,
      },
    );

    if (!updated) {
      return;
    }

    console.log(
      `Resultado recibido: ${event.eventType}`,
    );

    const steps = updated.get('steps') as Record<
      string,
      { status?: string }
    >;

    let finalStatus: 'ACTIVE' | 'FAILED' | null =
      null;

    // Si falla cualquiera de los servicios,
    // la activacion debe fallar.
    if (
      failed ||
      steps?.billing?.status === 'FAILED' ||
      steps?.provisioning?.status === 'FAILED'
    ) {
      finalStatus = 'FAILED';
    }

    // Ambos servicios deben confirmar OK.
    else if (
      steps?.billing?.status === 'OK' &&
      steps?.provisioning?.status === 'OK'
    ) {
      finalStatus = 'ACTIVE';
    }

    // Si falta algun resultado, esperar.
    if (!finalStatus) {
      console.log(
        `Saga ${activationId}: IN_PROGRESS`,
      );
      return;
    }

    // Intentar cerrar la saga una sola vez.
    const completed =
      await Activation.findOneAndUpdate(
        {
          _id: activationId,
          status: {
            $nin: ['ACTIVE', 'FAILED'],
          },
        },
        {
          $set: {
            status: finalStatus,
          },
        },
        {
          new: true,
        },
      );

    if (!completed) {
      return;
    }

    const finalEventType =
      finalStatus === 'ACTIVE'
        ? EVENT_TYPES.ACTIVATION_COMPLETED
        : EVENT_TYPES.ACTIVATION_FAILED;

    const finalEvent = {
      eventId: randomUUID(),
      eventType: finalEventType,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: activationId,
      customerId: event.customerId,
      source: 'activation-api',
      payload: {
        status: finalStatus,
      },
    };

    // Publicar resultado final en Kafka.
    await firstValueFrom(
      this.kafka.emit(
        TOPICS.ACTIVATION_EVENTS,
        {
          key: event.customerId,
          value: finalEvent,
        },
      ),
    );

    // Registrar evento final en el historial.
    await Activation.updateOne(
      {
        _id: activationId,
      },
      {
        $push: {
          history: {
            eventId: finalEvent.eventId,
            eventType: finalEventType,
            at: finalEvent.occurredAt,
          },
        },
      },
    );

    console.log(
      `Saga ${activationId}: ${finalStatus}`,
    );
  }

  // ========================================
  // CONSULTAR UNA ACTIVACION
  // ========================================

  async findOne(id: string) {
    const activation = await Activation.findById(
      id,
    ).lean();

    if (!activation) {
      throw new HttpException(
        'Activacion no encontrada',
        HttpStatus.NOT_FOUND,
      );
    }

    return activation;
  }
}

// ==========================================
// CONTROLADOR REST
// ==========================================

@Controller('activations')
class ActivationController {
  constructor(
    private readonly service: ActivationService,
  ) {}

  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  create(
    @Body()
    body: {
      customerId: string;
      planId: string;
      simulateFailure?: FailureMode;
    },
  ) {
    return this.service.create(body);
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.service.findOne(id);
  }
}

// ==========================================
// CONTROLADOR DE EVENTOS KAFKA
// ==========================================

@Controller()
class SagaController {
  constructor(
    private readonly service: ActivationService,
  ) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBilling(
    @Payload() event: any,
  ) {
    await this.service.processResult(event);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioning(
    @Payload() event: any,
  ) {
    await this.service.processResult(event);
  }
}

// ==========================================
// MODULO PRINCIPAL
// ==========================================

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_PRODUCER',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'activation-api',
            brokers: [
              process.env.KAFKA_BROKERS ||
                'kafka:19092',
            ],
          },
          consumer: {
            groupId: 'activation-api-producer',
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [
    ActivationController,
    SagaController,
  ],
  providers: [
    ActivationService,
  ],
})
class AppModule {}

// ==========================================
// INICIAR HTTP + KAFKA
// ==========================================

async function bootstrap() {
  const mongoUri =
    process.env.MONGO_URI ||
    'mongodb://mongo:27017/activation_api?replicaSet=rs0';

  await mongoose.connect(mongoUri);

  console.log(
    'MongoDB conectado correctamente',
  );

  const app = await NestFactory.create(
    AppModule,
  );

  app.enableCors();

  // Iniciar consumidor de resultados Kafka.
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'activation-api-saga',
        brokers: [
          process.env.KAFKA_BROKERS ||
            'kafka:19092',
        ],
      },
      consumer: {
        groupId: 'activation-api-saga',
      },
    },
  });

  await app.startAllMicroservices();

  console.log(
    'Consumidor Kafka de la saga iniciado',
  );

  // Iniciar servidor REST.
  const port = Number(
    process.env.PORT || 3000,
  );

  await app.listen(
    port,
    '0.0.0.0',
  );

  console.log(
    `Activation API funcionando en puerto ${port}`,
  );
}

bootstrap().catch((error) => {
  console.error(
    'Error iniciando activation-api:',
    error,
  );
  process.exit(1);
});
