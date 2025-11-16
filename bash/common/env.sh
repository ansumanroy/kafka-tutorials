#!/usr/bin/env bash
set -euo pipefail

# Load environment for Kafka connection.
# Prefers explicit MSK_ENV_FILE or LOCAL_ENV_FILE, falls back to infra/env.msk or infra/env.local.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/log.sh"

load_kafka_env() {
  local env_file=""

  if [[ -n "${MSK_ENV_FILE:-}" && -f "${MSK_ENV_FILE}" ]]; then
    env_file="$MSK_ENV_FILE"
    info "Using MSK env file: $env_file"
  elif [[ -n "${LOCAL_ENV_FILE:-}" && -f "${LOCAL_ENV_FILE}" ]]; then
    env_file="$LOCAL_ENV_FILE"
    info "Using local env file: $env_file"
  elif [[ -f "$ROOT_DIR/infra/env.msk" ]]; then
    env_file="$ROOT_DIR/infra/env.msk"
    info "Using default MSK env file: $env_file"
  elif [[ -f "$ROOT_DIR/infra/env.local" ]]; then
    env_file="$ROOT_DIR/infra/env.local"
    info "Using default local env file: $env_file"
  else
    error "No Kafka env file found. Create infra/env.msk or infra/env.local (see env-example files)."
    exit 1
  fi

  # shellcheck disable=SC1090
  source "$env_file"

  : "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS must be set in env file}"

  export KAFKA_BOOTSTRAP_SERVERS
  export KAFKA_SECURITY_PROTOCOL="${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}"
  export KAFKA_SASL_MECHANISM="${KAFKA_SASL_MECHANISM:-}"
  export KAFKA_SASL_USERNAME="${KAFKA_SASL_USERNAME:-}"
  export KAFKA_SASL_PASSWORD="${KAFKA_SASL_PASSWORD:-}"
  export KAFKA_ADDITIONAL_CONFIG="${KAFKA_ADDITIONAL_CONFIG:-}"
}

ensure_kafka_cli() {
  local missing=()
  for bin in kafka-topics.sh kafka-console-producer.sh kafka-console-consumer.sh kafka-consumer-groups.sh; do
    if ! command -v "$bin" >/dev/null 2>&1; then
      missing+=("$bin")
    fi
  done

  if ((${#missing[@]} > 0)); then
    warn "Missing Kafka CLI tools: ${missing[*]}"
    warn "Make sure Kafka bin directory is on your PATH (e.g., export PATH=\"$PATH:/path/to/kafka/bin\")."
  fi
}

