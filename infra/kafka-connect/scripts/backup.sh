#!/usr/bin/env bash
set -euo pipefail

# Orchestrate complete backup process: topic discovery, connector creation, config export, monitoring
# Usage: backup.sh <connect-rest-url> <s3-bucket> [topic-name]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# shellcheck source=../../bash/common/log.sh
source "$ROOT_DIR/bash/common/log.sh"

# Parse arguments
CONNECT_REST_URL="${1:?Kafka Connect REST API URL required (e.g., http://localhost:8083)}"
S3_BUCKET="${2:?S3 bucket name required}"
TOPIC_NAME="${3:-}"

# Generate backup ID (timestamp format: YYYYMMDD_HHMMSS)
BACKUP_ID=$(date +"%Y%m%d_%H%M%S")

info "═══════════════════════════════════════════════════════════"
info "Starting Kafka Backup Process"
info "═══════════════════════════════════════════════════════════"
info "Backup ID: $BACKUP_ID"
info "S3 Bucket: $S3_BUCKET"
info "Connect REST URL: $CONNECT_REST_URL"
if [[ -n "$TOPIC_NAME" ]]; then
  info "Topic: $TOPIC_NAME"
else
  info "Scope: All topics"
fi
info ""

# Step 1: Export topic configurations
info "Step 1/4: Exporting topic configurations..."
if ! "$SCRIPT_DIR/export-topic-configs.sh" "$S3_BUCKET" "$BACKUP_ID" "$TOPIC_NAME"; then
  error "Failed to export topic configurations"
  exit 1
fi
info "✓ Topic configurations exported"
info ""

# Step 2: Create S3 sink connectors
info "Step 2/4: Creating S3 sink connectors..."
if ! "$SCRIPT_DIR/create-sink-connectors.sh" "$CONNECT_REST_URL" "$S3_BUCKET" "$BACKUP_ID" "$TOPIC_NAME"; then
  error "Failed to create sink connectors"
  exit 1
fi
info "✓ Sink connectors created"
info ""

# Step 3: Wait for connectors to start and monitor progress
info "Step 3/4: Waiting for connectors to start processing..."
sleep 5

# Function to check connector status
check_connector_status() {
  local connector_name="$1"
  local status_response
  status_response=$(curl -s "$CONNECT_REST_URL/connectors/$connector_name/status" 2>/dev/null || echo "{}")
  
  local state
  state=$(echo "$status_response" | jq -r '.connector.state // "UNKNOWN"' 2>/dev/null || echo "UNKNOWN")
  
  echo "$state"
}

# Function to get connector tasks status
get_tasks_status() {
  local connector_name="$1"
  local status_response
  status_response=$(curl -s "$CONNECT_REST_URL/connectors/$connector_name/status" 2>/dev/null || echo "{}")
  
  echo "$status_response" | jq -r '.tasks[]?.state // empty' 2>/dev/null || echo ""
}

# Get list of connectors
if [[ -n "$TOPIC_NAME" ]]; then
  connector_name="camel-s3-sink-${TOPIC_NAME}"
  connector_name=$(echo "$connector_name" | sed 's/[^a-zA-Z0-9_-]/-/g')
  CONNECTORS=("$connector_name")
else
  # Get all connectors matching our pattern
  local connectors_response
  connectors_response=$(curl -s "$CONNECT_REST_URL/connectors" 2>/dev/null || echo "[]")
  readarray -t CONNECTORS < <(echo "$connectors_response" | jq -r '.[] | select(startswith("camel-s3-sink-"))' 2>/dev/null || echo "")
fi

info "Monitoring ${#CONNECTORS[@]} connector(s)..."
MAX_WAIT_TIME=300  # 5 minutes
WAIT_INTERVAL=10
elapsed=0
all_running=false

while [[ $elapsed -lt $MAX_WAIT_TIME ]]; do
  all_running=true
  
  for connector in "${CONNECTORS[@]}"; do
    state=$(check_connector_status "$connector")
    
    if [[ "$state" == "RUNNING" ]]; then
      info "  ✓ $connector: RUNNING"
    elif [[ "$state" == "FAILED" ]]; then
      error "  ✗ $connector: FAILED"
      all_running=false
    else
      info "  ⏳ $connector: $state"
      all_running=false
    fi
  done
  
  if [[ "$all_running" == "true" ]]; then
    info "✓ All connectors are RUNNING"
    break
  fi
  
  sleep $WAIT_INTERVAL
  elapsed=$((elapsed + WAIT_INTERVAL))
  info "Waiting... (${elapsed}s / ${MAX_WAIT_TIME}s)"
done

if [[ "$all_running" != "true" ]]; then
  warn "Some connectors may not be in RUNNING state after $MAX_WAIT_TIME seconds"
  warn "Continue monitoring manually or check connector logs"
fi
info ""

# Step 4: Create backup manifest
info "Step 4/4: Creating backup manifest..."

MANIFEST_FILE=$(mktemp)
cat > "$MANIFEST_FILE" <<EOF
{
  "backupId": "$BACKUP_ID",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "s3Bucket": "$S3_BUCKET",
  "s3Prefix": "backups/$BACKUP_ID",
  "topics": $(if [[ -n "$TOPIC_NAME" ]]; then echo "[\"$TOPIC_NAME\"]"; else echo "[]"; fi),
  "connectors": $(printf '%s\n' "${CONNECTORS[@]}" | jq -R . | jq -s .),
  "status": "in_progress"
}
EOF

aws s3 cp "$MANIFEST_FILE" "s3://$S3_BUCKET/backups/$BACKUP_ID/metadata/backup-manifest.json"
rm -f "$MANIFEST_FILE"

info "✓ Backup manifest created"
info ""

# Summary
info "═══════════════════════════════════════════════════════════"
info "Backup Process Initiated"
info "═══════════════════════════════════════════════════════════"
info "Backup ID: $BACKUP_ID"
info "S3 Location: s3://$S3_BUCKET/backups/$BACKUP_ID/"
info ""
info "Note: Connectors are now processing messages in the background."
info "Monitor connector status with:"
info "  curl $CONNECT_REST_URL/connectors/<connector-name>/status"
info ""
info "To stop connectors after backup completes:"
info "  for conn in ${CONNECTORS[*]}; do"
info "    curl -X DELETE $CONNECT_REST_URL/connectors/\$conn"
info "  done"
info ""

