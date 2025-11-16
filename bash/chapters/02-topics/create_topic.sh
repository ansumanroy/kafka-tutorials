#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"

TOPIC_NAME="${1:-demo-topic}"
PARTITIONS="${PARTITIONS:-1}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"

load_kafka_env
ensure_kafka_cli

info "Creating topic '$TOPIC_NAME' with $PARTITIONS partitions and replication factor $REPLICATION_FACTOR..."

if kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$TOPIC_NAME" >/dev/null 2>&1; then
  warn "Topic '$TOPIC_NAME' already exists; nothing to do."
  exit 0
fi

kafka-topics.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --create \
  --topic "$TOPIC_NAME" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR"

info "Topic '$TOPIC_NAME' created (or already existed)."
