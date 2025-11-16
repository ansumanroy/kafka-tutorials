#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <topic-name> [group-id]" >&2
  exit 1
fi

TOPIC_NAME="$1"
GROUP_ID="${2:-}"

load_kafka_env
ensure_kafka_cli

info "Describing topic '$TOPIC_NAME' partitions and offsets..."

kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --topic "$TOPIC_NAME"

if [[ -n "$GROUP_ID" ]]; then
  info "\nShowing consumer group '$GROUP_ID' offsets and lag..."
  kafka-consumer-groups.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --group "$GROUP_ID" \
    --describe
else
  info "\nNo consumer group id provided. Pass a group id as the second argument to see consumer offsets and lag."
fi
