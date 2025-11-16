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
ACKS="${KAFKA_ACKS:-1}"

load_kafka_env
ensure_kafka_cli

info "Starting interactive producer to topic  (acks=$ACKS)."
info "Type messages and press Ctrl+D when done."

kafka-console-producer.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic "$TOPIC_NAME" \
  --producer-property acks="$ACKS"
