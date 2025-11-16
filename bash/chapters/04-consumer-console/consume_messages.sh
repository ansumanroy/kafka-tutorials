#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <topic-name> [earliest|latest]" >&2
  exit 1
fi

TOPIC_NAME="$1"
OFFSET_MODE="${2:-latest}"

case "$OFFSET_MODE" in
  earliest|latest) ;;
  *)
    echo "Second argument must be 'earliest' or 'latest' (default: latest)" >&2
    exit 1
    ;;
esac

load_kafka_env
ensure_kafka_cli

info "Consuming messages from topic '$TOPIC_NAME' starting at $OFFSET_MODE offsets..."

kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic "$TOPIC_NAME" \
  --from-beginning=$([[ "$OFFSET_MODE" == "earliest" ]] && echo true || echo false)
