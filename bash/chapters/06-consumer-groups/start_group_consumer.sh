#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <topic-name> <group-id>" >&2
  exit 1
fi

TOPIC_NAME="$1"
GROUP_ID="$2"

load_kafka_env
ensure_kafka_cli

info "Starting consumer on topic '$TOPIC_NAME' in group '$GROUP_ID'..."

kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic "$TOPIC_NAME" \
  --group "$GROUP_ID" \
  --from-beginning
