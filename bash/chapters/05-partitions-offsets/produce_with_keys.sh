#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <topic-name>" >&2
  exit 1
fi

TOPIC_NAME="$1"

load_kafka_env
ensure_kafka_cli

info "Sending keyed messages to topic '$TOPIC_NAME' to demonstrate partitioning..."
info "Press Ctrl+C to stop."

# Produce a simple pattern of keys so you can see which partitions they go to.
{
  for i in $(seq 1 20); do
    key=$((i % 3))
    echo "key-$key:message-$i"
  done
} | kafka-console-producer.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic "$TOPIC_NAME" \
  --property parse.key=true \
  --property key.separator=:
