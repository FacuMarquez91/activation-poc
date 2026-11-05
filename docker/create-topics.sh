#!/bin/sh
set -eu

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:19092}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

for topic in \
  activation.requested \
  billing.events \
  provisioning.events \
  activation.events \
  activation.requested.dlq \
  billing.events.dlq \
  provisioning.events.dlq \
  activation.events.dlq
do
  echo "Creando/verificando topic: $topic"

  "$KAFKA_TOPICS" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000
done

echo "Todos los topics fueron verificados."
