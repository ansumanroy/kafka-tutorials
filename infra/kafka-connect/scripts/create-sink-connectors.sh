#!/usr/bin/env bash
set -euo pipefail

# Create S3 sink connectors for topics via Kafka Connect REST API
# Usage: create-sink-connectors.sh <connect-rest-url> <s3-bucket> <backup-id> [topic-name]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# shellcheck source=../../bash/common/log.sh
source "$ROOT_DIR/bash/common/log.sh"

# shellcheck source=../../bash/common/env.sh
source "$ROOT_DIR/bash/common/env.sh"

# Load Kafka environment
load_kafka_env

# Parse arguments
CONNECT_REST_URL="${1:?Kafka Connect REST API URL required (e.g., http://localhost:8083)}"
S3_BUCKET="${2:?S3 bucket name required}"
BACKUP_ID="${3:?Backup ID required (e.g., 20240101_120000)}"
TOPIC_NAME="${4:-}"

# Get AWS region (from environment or instance metadata)
AWS_REGION="${AWS_REGION:-}"
if [[ -z "$AWS_REGION" ]]; then
  AWS_REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
fi

CONNECTORS_DIR="$SCRIPT_DIR/../connectors"
CONNECTOR_TEMPLATE="$CONNECTORS_DIR/camel-s3-sink.json"

if [[ ! -f "$CONNECTOR_TEMPLATE" ]]; then
  error "Connector template not found: $CONNECTOR_TEMPLATE"
  exit 1
fi

info "Creating S3 sink connectors"
info "Connect REST URL: $CONNECT_REST_URL"
info "S3 Bucket: $S3_BUCKET"
info "Backup ID: $BACKUP_ID"
info "AWS Region: $AWS_REGION"

# Function to get topic partition count
get_topic_partitions() {
  local topic="$1"
  
  local partitions
  if [[ "$KAFKA_SECURITY_PROTOCOL" == "PLAINTEXT" ]]; then
    partitions=$(kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --describe --topic "$topic" 2>/dev/null | grep -E "Partition:" | wc -l | tr -d ' ')
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
    partitions=$(kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --command-config "$cmd_config_file" \
      --describe --topic "$topic" 2>/dev/null | grep -E "Partition:" | wc -l | tr -d ' ')
    rm -f "$cmd_config_file"
  fi
  
  echo "${partitions:-1}"
}

# Function to create connector for a topic
create_connector() {
  local topic="$1"
  local partitions
  partitions=$(get_topic_partitions "$topic")
  
  if [[ -z "$partitions" || "$partitions" == "0" ]]; then
    warn "Could not determine partition count for topic: $topic, using 1"
    partitions=1
  fi
  
  local connector_name="camel-s3-sink-${topic}"
  connector_name=$(echo "$connector_name" | sed 's/[^a-zA-Z0-9_-]/-/g')
  
  info "Creating connector: $connector_name for topic: $topic (partitions: $partitions)"
  
  # Read template and substitute variables
  local connector_config
  connector_config=$(cat "$CONNECTOR_TEMPLATE" | \
    sed "s|{{TOPIC_NAME}}|$topic|g" | \
    sed "s|{{PARTITION_COUNT}}|$partitions|g" | \
    sed "s|{{S3_BUCKET}}|$S3_BUCKET|g" | \
    sed "s|{{BACKUP_ID}}|$BACKUP_ID|g" | \
    sed "s|{{PARTITION}}|{partition}|g" | \
    sed "s|{{OFFSET}}|{offset}|g" | \
    sed "s|{{AWS_REGION}}|$AWS_REGION|g" | \
    sed "s|{{AWS_ACCESS_KEY}}||g" | \
    sed "s|{{AWS_SECRET_KEY}}||g")
  
  # Set connector name
  connector_config=$(echo "$connector_config" | jq ".name = \"$connector_name\"")
  
  # Create connector via REST API
  local response
  local status_code
  
  # Check if connector already exists
  response=$(curl -s -w "\n%{http_code}" \
    -X GET \
    "$CONNECT_REST_URL/connectors/$connector_name" 2>/dev/null || echo -e "\n404")
  
  status_code=$(echo "$response" | tail -n1)
  
  if [[ "$status_code" == "200" ]]; then
    warn "Connector $connector_name already exists, deleting it first"
    curl -s -X DELETE "$CONNECT_REST_URL/connectors/$connector_name" >/dev/null || true
    sleep 2
  fi
  
  # Create new connector
  response=$(curl -s -w "\n%{http_code}" \
    -X POST \
    -H "Content-Type: application/json" \
    -d "$connector_config" \
    "$CONNECT_REST_URL/connectors" 2>/dev/null)
  
  status_code=$(echo "$response" | tail -n1)
  
  if [[ "$status_code" == "201" || "$status_code" == "200" ]]; then
    info "Successfully created connector: $connector_name"
    return 0
  else
    error "Failed to create connector: $connector_name (HTTP $status_code)"
    echo "$response" | head -n -1 >&2
    return 1
  fi
}

# Create connectors
if [[ -n "$TOPIC_NAME" ]]; then
  # Create connector for single topic
  create_connector "$TOPIC_NAME"
else
  # Create connectors for all topics
  info "Discovering all topics..."
  
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
  
  if [[ -z "$topics_list" ]]; then
    error "Could not list topics from cluster"
    exit 1
  fi
  
  # Filter topics
  local topics
  readarray -t topics <<< "$topics_list"
  
  local created_count=0
  local failed_count=0
  
  for topic in "${topics[@]}"; do
    topic=$(echo "$topic" | tr -d '[:space:]')
    if [[ -z "$topic" ]]; then
      continue
    fi
    
    # Skip internal topics
    if [[ "$topic" =~ ^(__|_) ]]; then
      continue
    fi
    
    if create_connector "$topic"; then
      ((created_count++)) || true
    else
      ((failed_count++)) || true
    fi
  done
  
  info "Created $created_count connectors, $failed_count failed"
fi

info "Connector creation completed"

