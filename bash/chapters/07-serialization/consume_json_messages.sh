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

if command -v jq >/dev/null 2>&1; then
  info "Consuming JSON messages from '$TOPIC_NAME' and pretty-printing with jq..."
  kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --from-beginning | jq .
else
  warn "jq is not installed; printing raw messages. Install jq for pretty JSON output."
  kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --from-beginning
fi
