#!/usr/bin/env bash
set -euo pipefail

# Orchestrate complete restore process: topic recreation, message restoration, offset restoration
# Usage: restore.sh <s3-bucket> <backup-id> [topic-name] [consumer-group]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# shellcheck source=../../bash/common/log.sh
source "$ROOT_DIR/bash/common/log.sh"

# Parse arguments
S3_BUCKET="${1:?S3 bucket name required}"
BACKUP_ID="${2:?Backup ID required (e.g., 20240101_120000)}"
TOPIC_NAME="${3:-}"
CONSUMER_GROUP="${4:-}"

info "═══════════════════════════════════════════════════════════"
info "Starting Kafka Restore Process"
info "═══════════════════════════════════════════════════════════"
info "Backup ID: $BACKUP_ID"
info "S3 Bucket: $S3_BUCKET"
if [[ -n "$TOPIC_NAME" ]]; then
  info "Topic: $TOPIC_NAME"
else
  info "Scope: All topics"
fi
if [[ -n "$CONSUMER_GROUP" ]]; then
  info "Consumer Group: $CONSUMER_GROUP"
fi
info ""

# Verify backup exists
info "Verifying backup exists..."
if ! aws s3 ls "s3://$S3_BUCKET/backups/$BACKUP_ID/" >/dev/null 2>&1; then
  error "Backup not found: s3://$S3_BUCKET/backups/$BACKUP_ID/"
  exit 1
fi
info "✓ Backup verified"
info ""

# Step 1: Restore topic configurations and recreate topics
info "Step 1/3: Restoring topic configurations..."
if ! "$SCRIPT_DIR/restore-topics.sh" "$S3_BUCKET" "$BACKUP_ID" "$TOPIC_NAME"; then
  error "Failed to restore topic configurations"
  exit 1
fi
info "✓ Topics restored"
info ""

# Step 2: Restore messages
info "Step 2/3: Restoring messages from S3..."
OFFSET_MAPPINGS_FILE=$("$SCRIPT_DIR/restore-messages.sh" "$S3_BUCKET" "$BACKUP_ID" "$TOPIC_NAME")

if [[ ! -f "$OFFSET_MAPPINGS_FILE" ]]; then
  error "Failed to restore messages or generate offset mappings"
  exit 1
fi

info "✓ Messages restored"
info "Offset mappings: $OFFSET_MAPPINGS_FILE"
info ""

# Step 3: Restore consumer group offsets (if consumer group specified)
if [[ -n "$CONSUMER_GROUP" ]]; then
  info "Step 3/3: Restoring consumer group offsets..."
  if ! "$SCRIPT_DIR/restore-offsets.sh" "$OFFSET_MAPPINGS_FILE" "$CONSUMER_GROUP" "$TOPIC_NAME"; then
    warn "Failed to restore some consumer group offsets (non-critical)"
  else
    info "✓ Consumer group offsets restored"
  fi
else
  info "Step 3/3: Skipping consumer group offset restoration (no group specified)"
  info "  To restore offsets, run:"
  info "    restore-offsets.sh $OFFSET_MAPPINGS_FILE <consumer-group>"
fi
info ""

# Summary
info "═══════════════════════════════════════════════════════════"
info "Restore Process Complete"
info "═══════════════════════════════════════════════════════════"
info "Backup ID: $BACKUP_ID"
info "S3 Location: s3://$S3_BUCKET/backups/$BACKUP_ID/"
info "Offset mappings: $OFFSET_MAPPINGS_FILE"
info ""
info "To restore additional consumer group offsets:"
info "  restore-offsets.sh $OFFSET_MAPPINGS_FILE <consumer-group> [topic]"
info ""

