#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/docker-compose.kafka.yml"

echo "[info] Starting local Kafka using Docker Compose..."

docker compose -f "$COMPOSE_FILE" up -d

# Simple wait loop to check Kafka readiness
HOST="localhost"
PORT=9092

for i in {1..30}; do
  if nc -z "$HOST" "$PORT" >/dev/null 2>&1; then
    echo "[info] Kafka is up on $HOST:$PORT"
    echo "export KAFKA_BOOTSTRAP_SERVERS=$HOST:$PORT"
    exit 0
  fi
  echo "[info] Waiting for Kafka to become ready ($i/30)..."
  sleep 2
done

echo "[error] Kafka did not become ready in time." >&2
exit 1
