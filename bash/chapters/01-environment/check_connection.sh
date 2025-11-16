#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"

load_kafka_env
ensure_kafka_cli

info "Checking connectivity to Kafka at $KAFKA_BOOTSTRAP_SERVERS..."

# Try listing topics as a simple connectivity test
if kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list >/dev/null 2>&1; then
  info "Successfully connected to Kafka. CLI can list topics."
else
  error "Failed to connect to Kafka using KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS"
  error "Double-check network access, security protocol, and authentication settings."
  exit 1
fi
