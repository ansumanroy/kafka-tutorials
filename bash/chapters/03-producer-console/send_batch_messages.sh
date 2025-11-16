#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <topic-name> [count]" >&2
  exit 1
fi

TOPIC_NAME="$1"
COUNT="${2:-10}"
ACKS="${KAFKA_ACKS:-1}"

load_kafka_env
ensure_kafka_cli

info "Sending $COUNT messages to topic '$TOPIC_NAME' (acks=$ACKS)..."

{
  for i in $(seq 1 "$COUNT"); do
    echo "message-$i"
  done
} | kafka-console-producer.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic "$TOPIC_NAME" \
  --producer-property acks="$ACKS"

info "Done sending $COUNT messages to '$TOPIC_NAME'."
