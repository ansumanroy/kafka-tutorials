#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <group-id>" >&2
  exit 1
fi

GROUP_ID="$1"

load_kafka_env
ensure_kafka_cli

info "Checking consumer lag for group '$GROUP_ID'..."

kafka-consumer-groups.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --group "$GROUP_ID" \
  --describe
