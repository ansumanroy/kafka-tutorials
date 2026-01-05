#!/usr/bin/env bash
set -euo pipefail

# Restore topic configurations from S3 and recreate topics with original settings
# Usage: restore-topics.sh <s3-bucket> <backup-id> [topic-name]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# shellcheck source=../../bash/common/log.sh
source "$ROOT_DIR/bash/common/log.sh"

# shellcheck source=../../bash/common/env.sh
source "$ROOT_DIR/bash/common/env.sh"

# Load Kafka environment
load_kafka_env

# Check prerequisites
ensure_kafka_cli

# Parse arguments
S3_BUCKET="${1:?S3 bucket name required}"
BACKUP_ID="${2:?Backup ID required (e.g., 20240101_120000)}"
TOPIC_NAME="${3:-}"

# Create temporary directory
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

CONFIG_DIR="$TEMP_DIR/configs"
mkdir -p "$CONFIG_DIR"

info "Restoring topic configurations from backup"
info "S3 Bucket: $S3_BUCKET"
info "Backup ID: $BACKUP_ID"

# Download configurations from S3
info "Downloading topic configurations from S3..."
if ! aws s3 sync "s3://$S3_BUCKET/backups/$BACKUP_ID/configs/" "$CONFIG_DIR/" 2>/dev/null; then
  error "Failed to download configurations from S3"
  exit 1
fi

# Count config files
CONFIG_COUNT=$(find "$CONFIG_DIR" -name "*-config.json" | wc -l | tr -d ' ')
if [[ "$CONFIG_COUNT" == "0" ]]; then
  error "No topic configuration files found in backup"
  exit 1
fi

info "Found $CONFIG_COUNT topic configuration(s)"
info ""

# Function to restore a single topic
restore_topic() {
  local config_file="$1"
  local topic_config
  
  if [[ ! -f "$config_file" ]]; then
    warn "Config file not found: $config_file"
    return 1
  fi
  
  topic_config=$(cat "$config_file")
  
  local topic
  local partitions
  local replication_factor
  local configs
  
  topic=$(echo "$topic_config" | jq -r '.topic')
  partitions=$(echo "$topic_config" | jq -r '.partitions')
  replication_factor=$(echo "$topic_config" | jq -r '.replicationFactor')
  configs=$(echo "$topic_config" | jq -r '.configs // {}')
  
  if [[ -z "$topic" || "$topic" == "null" ]]; then
    warn "Invalid config file: $config_file (missing topic name)"
    return 1
  fi
  
  # Filter by topic name if specified
  if [[ -n "$TOPIC_NAME" && "$topic" != "$TOPIC_NAME" ]]; then
    return 0
  fi
  
  info "Restoring topic: $topic"
  info "  Partitions: $partitions"
  info "  Replication Factor: $replication_factor"
  
  # Check if topic already exists
  local topic_exists=false
  local topics_list
  
  if [[ "$KAFKA_SECURITY_PROTOCOL" == "PLAINTEXT" ]]; then
    topics_list=$(kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list 2>/dev/null || echo "")
  else
    local cmd_config_file=$(mktemp)
    cat > "$cmd_config_file" <<EOF
security.protocol=$KAFKA_SECURITY_PROTOCOL
EOF
    if [[ -n "$KAFKA_SASL_MECHANISM" ]]; then
      echo "sasl.mechanism=$KAFKA_SASL_MECHANISM" >> "$cmd_config_file"
      if [[ -n "$KAFKA_SASL_USERNAME" && -n "$KAFKA_SASL_PASSWORD" ]]; then
        if [[ "$KAFKA_SASL_MECHANISM" == *"SCRAM"* ]]; then
          echo "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$KAFKA_SASL_USERNAME\" password=\"$KAFKA_SASL_PASSWORD\";" >> "$cmd_config_file"
        else
          echo "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$KAFKA_SASL_USERNAME\" password=\"$KAFKA_SASL_PASSWORD\";" >> "$cmd_config_file"
        fi
      fi
    fi
    topics_list=$(kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --command-config "$cmd_config_file" --list 2>/dev/null || echo "")
    rm -f "$cmd_config_file"
  fi
  
  if echo "$topics_list" | grep -q "^${topic}$"; then
    topic_exists=true
    warn "Topic $topic already exists, skipping creation"
    info "  (To recreate, delete the topic first)"
  else
    # Build create topic command
    local create_cmd="kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS"
    create_cmd="$create_cmd --create --topic $topic"
    create_cmd="$create_cmd --partitions $partitions"
    create_cmd="$create_cmd --replication-factor $replication_factor"
    
    # Add configs
    local config_cmd=""
    if [[ "$configs" != "null" && "$configs" != "{}" ]]; then
      local config_pairs
      config_pairs=$(echo "$configs" | jq -r 'to_entries[] | "\(.key)=\(.value)"' | tr '\n' ',' | sed 's/,$//')
      
      if [[ -n "$config_pairs" ]]; then
        create_cmd="$create_cmd --config $config_pairs"
      fi
    fi
    
    # Add security config if needed
    if [[ "$KAFKA_SECURITY_PROTOCOL" != "PLAINTEXT" ]]; then
      local cmd_config_file=$(mktemp)
      cat > "$cmd_config_file" <<EOF
security.protocol=$KAFKA_SECURITY_PROTOCOL
EOF
      if [[ -n "$KAFKA_SASL_MECHANISM" ]]; then
        echo "sasl.mechanism=$KAFKA_SASL_MECHANISM" >> "$cmd_config_file"
        if [[ -n "$KAFKA_SASL_USERNAME" && -n "$KAFKA_SASL_PASSWORD" ]]; then
          if [[ "$KAFKA_SASL_MECHANISM" == *"SCRAM"* ]]; then
            echo "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$KAFKA_SASL_USERNAME\" password=\"$KAFKA_SASL_PASSWORD\";" >> "$cmd_config_file"
          else
            echo "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$KAFKA_SASL_USERNAME\" password=\"$KAFKA_SASL_PASSWORD\";" >> "$cmd_config_file"
          fi
        fi
      fi
      create_cmd="$create_cmd --command-config $cmd_config_file"
    fi
    
    # Execute create command
    if eval "$create_cmd" 2>/dev/null; then
      info "  ✓ Topic created successfully"
    else
      error "  ✗ Failed to create topic: $topic"
      return 1
    fi
  fi
  
  # Update topic configurations if topic exists and configs are provided
  if [[ "$topic_exists" == "true" && "$configs" != "null" && "$configs" != "{}" ]]; then
    info "  Updating topic configurations..."
    
    local config_cmd="kafka-configs.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS"
    config_cmd="$config_cmd --alter --entity-type topics --entity-name $topic"
    
    # Add each config
    while IFS='=' read -r key value; do
      if [[ -n "$key" && -n "$value" ]]; then
        config_cmd="$config_cmd --add-config ${key}=${value}"
      fi
    done < <(echo "$configs" | jq -r 'to_entries[] | "\(.key)=\(.value)"')
    
    if [[ "$KAFKA_SECURITY_PROTOCOL" != "PLAINTEXT" ]]; then
      local cmd_config_file=$(mktemp)
      cat > "$cmd_config_file" <<EOF
security.protocol=$KAFKA_SECURITY_PROTOCOL
EOF
      if [[ -n "$KAFKA_SASL_MECHANISM" ]]; then
        echo "sasl.mechanism=$KAFKA_SASL_MECHANISM" >> "$cmd_config_file"
        if [[ -n "$KAFKA_SASL_USERNAME" && -n "$KAFKA_SASL_PASSWORD" ]]; then
          if [[ "$KAFKA_SASL_MECHANISM" == *"SCRAM"* ]]; then
            echo "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$KAFKA_SASL_USERNAME\" password=\"$KAFKA_SASL_PASSWORD\";" >> "$cmd_config_file"
          else
            echo "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$KAFKA_SASL_USERNAME\" password=\"$KAFKA_SASL_PASSWORD\";" >> "$cmd_config_file"
          fi
        fi
      fi
      config_cmd="$config_cmd --command-config $cmd_config_file"
    fi
    
    if eval "$config_cmd" 2>/dev/null; then
      info "  ✓ Topic configurations updated"
    else
      warn "  ⚠ Failed to update some configurations (may be read-only)"
    fi
  fi
  
  return 0
}

# Restore topics
RESTORED_COUNT=0
FAILED_COUNT=0

for config_file in "$CONFIG_DIR"/*-config.json; do
  if [[ -f "$config_file" ]]; then
    if restore_topic "$config_file"; then
      ((RESTORED_COUNT++)) || true
    else
      ((FAILED_COUNT++)) || true
    fi
    echo ""
  fi
done

info "═══════════════════════════════════════════════════════════"
info "Topic Restoration Complete"
info "═══════════════════════════════════════════════════════════"
info "Restored: $RESTORED_COUNT"
if [[ $FAILED_COUNT -gt 0 ]]; then
  warn "Failed: $FAILED_COUNT"
  exit 1
fi

