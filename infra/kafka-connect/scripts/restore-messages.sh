#!/usr/bin/env bash
set -euo pipefail

# Wrapper script to restore messages using Python script
# Usage: restore-messages.sh <s3-bucket> <backup-id> [topic-name]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# shellcheck source=../../bash/common/log.sh
source "$ROOT_DIR/bash/common/log.sh"

# shellcheck source=../../bash/common/env.sh
source "$ROOT_DIR/bash/common/env.sh"

# Load Kafka environment
load_kafka_env

# Export Kafka config for Python script
export KAFKA_BOOTSTRAP_SERVERS
export KAFKA_SECURITY_PROTOCOL="${KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}"
export KAFKA_SASL_MECHANISM="${KAFKA_SASL_MECHANISM:-}"
export KAFKA_SASL_USERNAME="${KAFKA_SASL_USERNAME:-}"
export KAFKA_SASL_PASSWORD="${KAFKA_SASL_PASSWORD:-}"

# Parse arguments
S3_BUCKET="${1:?S3 bucket name required}"
BACKUP_ID="${2:?Backup ID required (e.g., 20240101_120000)}"
TOPIC_NAME="${3:-}"

PYTHON_SCRIPT="$SCRIPT_DIR/restore-messages.py"
OUTPUT_DIR="/tmp/kafka-restore-${BACKUP_ID}"

# Check if Python script exists
if [[ ! -f "$PYTHON_SCRIPT" ]]; then
  error "Python restore script not found: $PYTHON_SCRIPT"
  exit 1
fi

# Check if kafka-python is installed
if ! python3 -c "import kafka" 2>/dev/null; then
  error "kafka-python library not installed"
  error "Install with: pip3 install kafka-python boto3"
  exit 1
fi

info "Restoring messages from backup"
info "S3 Bucket: $S3_BUCKET"
info "Backup ID: $BACKUP_ID"
if [[ -n "$TOPIC_NAME" ]]; then
  info "Topic: $TOPIC_NAME"
fi
info ""

# Run Python script
if [[ -n "$TOPIC_NAME" ]]; then
  python3 "$PYTHON_SCRIPT" "$S3_BUCKET" "$BACKUP_ID" --topic "$TOPIC_NAME" --output-dir "$OUTPUT_DIR"
else
  python3 "$PYTHON_SCRIPT" "$S3_BUCKET" "$BACKUP_ID" --output-dir "$OUTPUT_DIR"
fi

# Export offset mappings path for restore-offsets script
echo "$OUTPUT_DIR/offset-mappings.json"

