#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"

load_kafka_env
ensure_kafka_cli

info "Listing topics on $KAFKA_BOOTSTRAP_SERVERS..."

kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list
