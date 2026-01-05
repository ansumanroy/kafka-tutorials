#!/usr/bin/env bash
set -euo pipefail

# Export topic configurations from MSK cluster to S3
# Usage: export-topic-configs.sh <s3-bucket> <backup-id> [topic-name]

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

# Build kafka-topics.sh command
KAFKA_TOPICS_CMD="kafka-topics.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS"

# Add security configuration if needed
if [[ "$KAFKA_SECURITY_PROTOCOL" != "PLAINTEXT" ]]; then
  KAFKA_TOPICS_CMD="$KAFKA_TOPICS_CMD --command-config <(echo 'security.protocol=$KAFKA_SECURITY_PROTOCOL"
  if [[ -n "$KAFKA_SASL_MECHANISM" ]]; then
    KAFKA_TOPICS_CMD="$KAFKA_TOPICS_CMD
sasl.mechanism=$KAFKA_SASL_MECHANISM"
    if [[ -n "$KAFKA_SASL_USERNAME" && -n "$KAFKA_SASL_PASSWORD" ]]; then
      if [[ "$KAFKA_SASL_MECHANISM" == *"SCRAM"* ]]; then
        KAFKA_TOPICS_CMD="$KAFKA_TOPICS_CMD
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$KAFKA_SASL_USERNAME\" password=\"$KAFKA_SASL_PASSWORD\";"
      else
        KAFKA_TOPICS_CMD="$KAFKA_TOPICS_CMD
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$KAFKA_SASL_USERNAME\" password=\"$KAFKA_SASL_PASSWORD\";"
      fi
    fi
  fi
  KAFKA_TOPICS_CMD="$KAFKA_TOPICS_CMD')"
fi

# Create temporary directory
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

CONFIG_DIR="$TEMP_DIR/configs"
mkdir -p "$CONFIG_DIR"

info "Exporting topic configurations to S3 bucket: $S3_BUCKET"
info "Backup ID: $BACKUP_ID"

# Function to export a single topic configuration
export_topic_config() {
  local topic="$1"
  local config_file="$CONFIG_DIR/${topic}-config.json"
  
  info "Exporting configuration for topic: $topic"
  
  # Get topic description
  local describe_output
  if [[ "$KAFKA_SECURITY_PROTOCOL" == "PLAINTEXT" ]]; then
    describe_output=$(eval "$KAFKA_TOPICS_CMD --describe --topic $topic" 2>/dev/null || echo "")
  else
    # For non-PLAINTEXT, we need to handle command config differently
    local cmd_config_file="$TEMP_DIR/cmd-config.properties"
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
    describe_output=$(eval "$KAFKA_TOPICS_CMD --command-config $cmd_config_file --describe --topic $topic" 2>/dev/null || echo "")
  fi
  
  if [[ -z "$describe_output" ]]; then
    warn "Could not get configuration for topic: $topic"
    return 1
  fi
  
  # Parse topic description and create JSON
  local partitions=0
  local replication_factor=0
  local configs="{}"
  
  # Extract partitions and replication factor from first partition line
  local first_partition_line
  first_partition_line=$(echo "$describe_output" | grep -E "Partition:" | head -1)
  
  if [[ -n "$first_partition_line" ]]; then
    partitions=$(echo "$describe_output" | grep -E "Partition:" | wc -l | tr -d ' ')
    
    # Extract replication factor
    if echo "$first_partition_line" | grep -qE "Replicas:"; then
      replication_factor=$(echo "$first_partition_line" | sed -n 's/.*Replicas: \([0-9]*\).*/\1/p' | head -1)
      if [[ -z "$replication_factor" ]]; then
        # Try alternative format
        replication_factor=$(echo "$first_partition_line" | grep -oP 'Replicas: \K[0-9]+' | head -1 || echo "0")
      fi
    fi
  fi
  
  # Extract topic configs (retention, compression, etc.)
  local config_lines
  config_lines=$(echo "$describe_output" | grep -E "Configs:" || true)
  
  if [[ -n "$config_lines" ]]; then
    local config_json="{"
    local first=true
    
    # Parse configs - format: Configs: retention.ms=604800000,segment.ms=604800000
    local configs_str
    configs_str=$(echo "$config_lines" | sed -n 's/.*Configs: \(.*\)/\1/p' | head -1)
    
    if [[ -n "$configs_str" ]]; then
      IFS=',' read -ra CONFIG_ARR <<< "$configs_str"
      for config_pair in "${CONFIG_ARR[@]}"; do
        if [[ "$config_pair" =~ ^([^=]+)=(.*)$ ]]; then
          local key="${BASH_REMATCH[1]}"
          local value="${BASH_REMATCH[2]}"
          if [[ "$first" == "true" ]]; then
            first=false
          else
            config_json+=","
          fi
          # Escape quotes in value
          value=$(echo "$value" | sed 's/"/\\"/g')
          config_json+="\"$key\":\"$value\""
        fi
      done
      config_json+="}"
      configs="$config_json"
    fi
  fi
  
  # Create JSON file
  cat > "$config_file" <<EOF
{
  "topic": "$topic",
  "partitions": $partitions,
  "replicationFactor": $replication_factor,
  "configs": $configs,
  "exportedAt": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
  
  info "Exported configuration for topic: $topic (partitions: $partitions, replication: $replication_factor)"
}

# Export topic(s)
if [[ -n "$TOPIC_NAME" ]]; then
  # Export single topic
  export_topic_config "$TOPIC_NAME"
else
  # Export all topics
  info "Discovering all topics..."
  
  local topics_list
  if [[ "$KAFKA_SECURITY_PROTOCOL" == "PLAINTEXT" ]]; then
    topics_list=$(eval "$KAFKA_TOPICS_CMD --list" 2>/dev/null || echo "")
  else
    local cmd_config_file="$TEMP_DIR/cmd-config.properties"
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
    topics_list=$(eval "$KAFKA_TOPICS_CMD --command-config $cmd_config_file --list" 2>/dev/null || echo "")
  fi
  
  if [[ -z "$topics_list" ]]; then
    error "Could not list topics from cluster"
    exit 1
  fi
  
  # Filter out internal topics (optional - can be configured)
  local topics
  readarray -t topics <<< "$topics_list"
  
  local exported_count=0
  for topic in "${topics[@]}"; do
    topic=$(echo "$topic" | tr -d '[:space:]')
    if [[ -z "$topic" ]]; then
      continue
    fi
    
    # Skip internal topics (__connect_*, _schemas, etc.)
    if [[ "$topic" =~ ^(__|_) ]]; then
      continue
    fi
    
    if export_topic_config "$topic"; then
      ((exported_count++)) || true
    fi
  done
  
  info "Exported configurations for $exported_count topics"
fi

# Upload to S3
info "Uploading configurations to S3..."
aws s3 sync "$CONFIG_DIR/" "s3://$S3_BUCKET/backups/$BACKUP_ID/configs/" --delete

info "Topic configurations exported successfully"
info "S3 location: s3://$S3_BUCKET/backups/$BACKUP_ID/configs/"

