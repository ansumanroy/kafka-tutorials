#!/usr/bin/env bash
# Wrapper script to run Kafka CLI commands via Docker
# This allows running Kafka scripts without installing Kafka locally

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka-tutorials-kafka}"
COMMAND_NAME=$(basename "$0")

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
    echo "Error: Kafka container '${KAFKA_CONTAINER}' is not running" >&2
    echo "Run 'make kafka-apache-start' first" >&2
    exit 1
fi

# Execute the Kafka command inside the container
docker exec -i "${KAFKA_CONTAINER}" "${COMMAND_NAME}" "$@"
