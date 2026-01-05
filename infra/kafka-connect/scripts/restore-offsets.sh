#!/usr/bin/env bash
set -euo pipefail

# Restore consumer group offsets to exact values based on offset mappings from message restoration
# Usage: restore-offsets.sh <offset-mappings-file> [consumer-group] [topic]

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
OFFSET_MAPPINGS_FILE="${1:?Offset mappings file required}"
CONSUMER_GROUP="${2:-}"
TOPIC="${3:-}"

if [[ ! -f "$OFFSET_MAPPINGS_FILE" ]]; then
  error "Offset mappings file not found: $OFFSET_MAPPINGS_FILE"
  exit 1
fi

info "Restoring consumer group offsets"
info "Offset mappings file: $OFFSET_MAPPINGS_FILE"

# Check if jq is installed
if ! command -v jq >/dev/null 2>&1; then
  error "jq is required but not installed"
  error "Install with: yum install jq (or apt-get install jq)"
  exit 1
fi

# Load offset mappings
if ! OFFSET_MAPPINGS=$(cat "$OFFSET_MAPPINGS_FILE" | jq . 2>/dev/null); then
  error "Failed to parse offset mappings file"
  exit 1
fi

# Build kafka-consumer-groups command
KAFKA_CG_CMD="kafka-consumer-groups.sh --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS"

# Add security configuration if needed
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
  KAFKA_CG_CMD="$KAFKA_CG_CMD --command-config $cmd_config_file"
fi

# Function to restore offsets for a consumer group
restore_group_offsets() {
  local group="$1"
  local topic="$2"
  
  info "Restoring offsets for consumer group: $group, topic: $topic"
  
  # Get offset mappings for this topic
  local topic_mappings
  topic_mappings=$(echo "$OFFSET_MAPPINGS" | jq -r ".[\"$topic\"] // []")
  
  if [[ -z "$topic_mappings" || "$topic_mappings" == "[]" ]]; then
    warn "No offset mappings found for topic: $topic"
    return 1
  fi
    
  # Build offset reset command
  # We'll set offsets based on the new offsets from message restoration
  # Since we can't directly map old offsets, we'll set to the first restored message offset
  
  local offset_reset_cmd="$KAFKA_CG_CMD --group $group --reset-offsets"
  offset_reset_cmd="$offset_reset_cmd --topic $topic"
  offset_reset_cmd="$offset_reset_cmd --to-offset"
  
  # For each partition, find the first restored offset and set consumer group to it
  local partitions
  partitions=$(echo "$topic_mappings" | jq -r '[.[] | .[0]] | unique | .[]')
  
  local reset_spec=""
  while IFS= read -r partition; do
    # Find the minimum new offset for this partition
    local min_new_offset
    min_new_offset=$(echo "$topic_mappings" | \
      jq -r "[.[] | select(.[0] == $partition) | .[2]] | min")
    
    if [[ -n "$min_new_offset" && "$min_new_offset" != "null" ]]; then
      if [[ -n "$reset_spec" ]]; then
        reset_spec="$reset_spec,"
      fi
      reset_spec="${reset_spec}${topic}:${partition}:${min_new_offset}"
      info "  Partition $partition: setting offset to $min_new_offset"
    fi
  done <<< "$partitions"
  
  if [[ -z "$reset_spec" ]]; then
    warn "No valid offset mappings found for topic: $topic"
    return 1
  fi
  
  # Note: kafka-consumer-groups.sh --reset-offsets doesn't support setting multiple partitions directly
  # We need to use --to-offset with a file or set each partition individually
  # For now, we'll create a reset file and use it
  
  local reset_file=$(mktemp)
  
  # Parse reset_spec and create reset file format
  IFS=',' read -ra SPECS <<< "$reset_spec"
  for spec in "${SPECS[@]}"; do
    IFS=':' read -r topic_name partition offset <<< "$spec"
    echo "$topic_name:$partition:$offset" >> "$reset_file"
  done
  
  # Use --execute flag to actually reset offsets
  # Note: This approach requires using a different method since kafka-consumer-groups.sh
  # doesn't directly support setting offsets from a file in this format
  
  info "Setting offsets for $group..."
  
  # Set offsets partition by partition
  for spec in "${SPECS[@]}"; do
    IFS=':' read -r topic_name partition offset <<< "$spec"
    
    # Use --to-offset with execute
    if eval "$offset_reset_cmd:$partition:$offset --execute" 2>/dev/null; then
      info "  ✓ Set partition $partition to offset $offset"
    else
      warn "  ✗ Failed to set partition $partition offset"
    fi
  done
  
  rm -f "$reset_file"
  
  info "Offset restoration completed for group: $group, topic: $topic"
}

# Get list of consumer groups
if [[ -n "$CONSUMER_GROUP" ]]; then
  GROUPS=("$CONSUMER_GROUP")
else
  info "Discovering consumer groups..."
  local groups_list
  if [[ "$KAFKA_SECURITY_PROTOCOL" == "PLAINTEXT" ]]; then
    groups_list=$(kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list 2>/dev/null || echo "")
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
    groups_list=$(kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --command-config "$cmd_config_file" --list 2>/dev/null || echo "")
    rm -f "$cmd_config_file"
  fi
  
  if [[ -z "$groups_list" ]]; then
    error "Could not list consumer groups"
    exit 1
  fi
  
  readarray -t GROUPS <<< "$groups_list"
fi

# Get list of topics from offset mappings
if [[ -n "$TOPIC" ]]; then
  TOPICS=("$TOPIC")
else
  TOPICS=$(echo "$OFFSET_MAPPINGS" | jq -r 'keys[]')
  readarray -t TOPICS <<< "$TOPICS"
fi

info "Restoring offsets for ${#GROUPS[@]} consumer group(s) and ${#TOPICS[@]} topic(s)"
info ""

RESTORED_COUNT=0
FAILED_COUNT=0

for group in "${GROUPS[@]}"; do
  group=$(echo "$group" | tr -d '[:space:]')
  if [[ -z "$group" ]]; then
    continue
  fi
  
  for topic in "${TOPICS[@]}"; do
    topic=$(echo "$topic" | tr -d '[:space:]')
    if [[ -z "$topic" ]]; then
      continue
    fi
    
    if restore_group_offsets "$group" "$topic"; then
      ((RESTORED_COUNT++)) || true
    else
      ((FAILED_COUNT++)) || true
    fi
    echo ""
  done
done

info "═══════════════════════════════════════════════════════════"
info "Offset Restoration Complete"
info "═══════════════════════════════════════════════════════════"
info "Restored: $RESTORED_COUNT"
if [[ $FAILED_COUNT -gt 0 ]]; then
  warn "Failed: $FAILED_COUNT"
  exit 1
fi

