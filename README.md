# Activation POC — Activación de servicios con Kafka

Prueba de concepto de una plataforma de activación de servicios basada en microservicios, eventos de Apache Kafka y una saga con compensación. Incluye una interfaz React para iniciar activaciones y visualizar su evolución.

## Arquitectura

| Componente | Responsabilidad |
| --- | --- |
| `activation-api` | API HTTP para solicitar y consultar activaciones; coordina la saga y publica los resultados finales. Expone actualizaciones por Socket.IO. |
| `billing-service` | Crea cuentas de facturación y compensa (cancela) la cuenta ante una activación fallida. |
| `provisioning-service` | Simula el aprovisionamiento y publica el resultado. |
| `notification-service` | Consume resultados finales y envía correos a MailHog. |
| `crm-analytics-service` | Audita eventos de negocio en MongoDB. |
| `apps/demo-ui` | Frontend React + Vite para iniciar activaciones y seguir la saga. |

Infraestructura: Apache Kafka 3.9.1 (KRaft), MongoDB 7 con replica set `rs0`, Kafka UI, MailHog y Docker Compose.

### Flujo de la saga

1. La interfaz o un cliente HTTP envía `POST /activations`; la API responde `202 Accepted` con un `activationId` y estado `PENDING`.
2. `activation-api` publica `ActivationRequested` en `activation.requested`.
3. Billing y Provisioning consumen la solicitud y publican sus resultados de forma independiente.
4. La API consolida los resultados y publica `ActivationCompleted` (`ACTIVE`) o `ActivationFailed` (`FAILED`) en `activation.events`.
5. Ante `ActivationFailed`, Billing cancela la cuenta creada (o registra una cancelación preventiva) y emite `BillingAccountCancelled`.
6. Notification envía el mensaje correspondiente a MailHog; CRM guarda los eventos de auditoría.

Los eventos contienen `eventId`, `eventType`, `version`, `occurredAt`, `correlationId` (identificador de activación), `customerId`, `source` y `payload`. Se utiliza `customerId` como clave del mensaje.

## Topics

| Topic de negocio | DLQ correspondiente |
| --- | --- |
| `activation.requested` | `activation.requested.dlq` |
| `billing.events` | `billing.events.dlq` |
| `provisioning.events` | `provisioning.events.dlq` |
| `activation.events` | `activation.events.dlq` |

En el entorno de prueba se configuraron tres particiones por topic, un factor de replicación de uno y retención de siete días.

## Requisitos y ejecución local

- Docker Desktop y Docker Compose.
- Node.js y npm para iniciar el frontend React en desarrollo.

Desde la raíz del repositorio, iniciar los servicios definidos en Compose:

```bash
docker compose -f docker-compose.yml -f compose.storage.yml -f compose.apps.yml up -d --build
```

Consultar el estado y los registros:

```bash
docker compose -f docker-compose.yml -f compose.storage.yml -f compose.apps.yml ps
docker compose -f docker-compose.yml -f compose.storage.yml -f compose.apps.yml logs --tail=60 activation-api
```

Iniciar la interfaz (en otra terminal):

```bash
cd apps/demo-ui
npm ci
npm run dev
```

### Direcciones locales

| Recurso | Dirección |
| --- | --- |
| Interfaz React | <http://localhost:5173> |
| API HTTP y Socket.IO | <http://localhost:3000> |
| Kafka UI | <http://localhost:8080> |
| MailHog | <http://localhost:8025> |

**Nota de instalación:** el entorno utilizado en las pruebas tenía el replica set de MongoDB y los ocho topics inicializados. En una instalación totalmente nueva, verificar y ejecutar los pasos de inicialización definidos en `compose.init.yml` y `docker/create-topics.sh` antes de realizar las demostraciones. No asumir que un simple `up` crea todos los topics automáticamente.

## Uso de la API

Crear una activación:

```bash
curl -i -X POST http://localhost:3000/activations \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"C-DEMO-001","planId":"PLAN-FIBRA","simulateFailure":"none"}'
```

Consultar la activación con el ID devuelto:

```bash
curl http://localhost:3000/activations/REEMPLAZAR_ACTIVATION_ID
```

Valores de `simulateFailure`: `none`, `billing` o `provisioning`. Los fallos simulados son resultados de negocio válidos; no deben confundirse con fallas técnicas destinadas a DLQ.

## Demostraciones realizadas

| Escenario | Resultado observado |
| --- | --- |
| Activación exitosa | `C-9022` terminó `ACTIVE`, con resultados correctos de Billing y Provisioning. |
| Fallo y compensación | `C-9024` terminó `FAILED`; Billing publicó `BillingAccountCancelled`. |
| Reintentos y DLQ | En Provisioning, un mensaje malformado `C-TEST-DLQ-002` provocó tres reintentos (1, 2 y 4 s), agotamiento y publicación en `activation.requested.dlq`. |
| Reinicio de Notification | Con Notification detenido, se generó `C-9040`; tras volver a iniciarlo se observó su correo de activación en MailHog. |
| Escalado de Billing | Dos réplicas compartieron seis asignaciones de particiones de `activation.requested` y `activation.events` (tres cada una); luego se volvió a una réplica. |
| Replay | Un grupo independiente `loyalty-replay-demo-v1` leyó cinco eventos históricos de `activation.events` con `--from-beginning`. |

Para observar los grupos y offsets desde Git Bash en Windows, deshabilitar la conversión de rutas MSYS:

```bash
MSYS_NO_PATHCONV=1 docker compose -f docker-compose.yml -f compose.storage.yml -f compose.apps.yml exec kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group billing-svc-server
```

Para escalar Billing durante una demostración:

```bash
docker compose -f docker-compose.yml -f compose.storage.yml -f compose.apps.yml up -d --no-deps --scale billing-service=2 billing-service
```

Y volver a una réplica:

```bash
docker compose -f docker-compose.yml -f compose.storage.yml -f compose.apps.yml up -d --no-deps --scale billing-service=1 billing-service
```

## Garantías y limitaciones conocidas

- **Deduplicación parcial:** Billing y Provisioning tienen índices únicos para `activationId`, y CRM tiene índice único para `eventId`. Sin embargo, la republicación de eventos de salida ante reentregas todavía puede generar duplicados con nuevos `eventId`.
- **Reintentos/DLQ:** se incorporaron en Billing y Provisioning. Se verificaron experimentalmente los reintentos y la publicación en DLQ de Provisioning, no todos los caminos de error de los cinco servicios.
- **Offsets:** los consumidores utilizan actualmente el manejo predeterminado de NestJS/KafkaJS; no se implementó confirmación manual de offsets en todos los servicios.
- **Transactional Outbox:** no implementado; se mantuvo fuera de la versión entregable.
- **Replay:** demostrado mediante un consumidor de consola con grupo independiente; no se implementó un servicio Loyalty permanente.
- **Mensajería entre MongoDB y Kafka:** no existe una transacción distribuida de extremo a extremo, por lo que no se afirma garantía *exactly-once*.

## Compilación

Desde la raíz:

```bash
npx nest build activation-api
npx nest build billing-service
npx nest build provisioning-service
npx nest build notification-service
npx nest build crm-analytics-service
```

Para compilar React:

```bash
cd apps/demo-ui
npm run build
```

Las pruebas descritas se ejecutaron en un entorno local de desarrollo. El proyecto es una prueba de concepto y no debe presentarse como sistema endurecido para producción.
