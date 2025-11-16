#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <topic-name> [--force]" >&2
  exit 1
fi

TOPIC_NAME="$1"
FORCE="${2:-}" # optional --force flag

load_kafka_env
ensure_kafka_cli

if [[ "$FORCE" != "--force" ]]; then
  warn "You are about to delete topic '$TOPIC_NAME'. This is irreversible."
  warn "Re-run with --force to proceed: $0 $TOPIC_NAME --force"
  exit 1
fi

info "Deleting topic '$TOPIC_NAME'..."

kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --delete --topic "$TOPIC_NAME"

info "Delete command issued for topic '$TOPIC_NAME'. Actual deletion may be asynchronous."
