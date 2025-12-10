#!/usr/bin/env bash
#
# demo_partitioning.sh
#
# Demonstrate how Kafka partitioning works with message keys.
# Shows that messages with same key go to same partition.
#
# Usage:
#   ./demo_partitioning.sh [topic_name] [num_partitions]
#
# Examples:
#   ./demo_partitioning.sh                          # Use defaults
#   ./demo_partitioning.sh partition-demo 6        # Custom topic
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
TOPIC_NAME="${1:-partitioning-demo}"
NUM_PARTITIONS="${2:-3}"
REPLICATION_FACTOR=1

log_section() {
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "════════════════════════════════════════════════════════════"
}

log_demo() {
  echo ""
  echo "────────────────────────────────────────────────────────────"
  echo "  $1"
  echo "────────────────────────────────────────────────────────────"
}

create_topic() {
  log_info "Creating topic: $TOPIC_NAME with $NUM_PARTITIONS partitions"
  
  # Delete if exists
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  
  sleep 2
  
  # Create fresh
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions "$NUM_PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR"
  
  sleep 2
  log_success "Topic created with $NUM_PARTITIONS partitions"
}

demo_same_key_same_partition() {
  log_demo "Demo 1: Same Key → Same Partition"
  
  echo "Sending 10 messages with key 'user-A'..."
  
  for i in {1..10}; do
    echo "user-A:Message $i from user A"
  done | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --property "parse.key=true" \
    --property "key.separator=:" \
    2>/dev/null
  
  sleep 2
  
  log_info "Consuming messages to show partition assignment..."
  
  timeout 3 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --from-beginning \
    --property print.key=true \
    --property print.partition=true \
    --property key.separator=" | " 2>/dev/null || true
  
  echo ""
  log_success "Notice: All 'user-A' messages went to the SAME partition"
}

demo_different_keys() {
  log_demo "Demo 2: Different Keys → Different Partitions"
  
  echo "Sending messages from multiple users..."
  
  for user in A B C D E; do
    for i in {1..3}; do
      echo "user-${user}:Message $i from user $user"
    done
  done | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --property "parse.key=true" \
    --property "key.separator=:" \
    2>/dev/null
  
  sleep 2
  
  log_info "Consuming messages to show distribution..."
  
  timeout 5 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --from-beginning \
    --property print.key=true \
    --property print.partition=true \
    --property key.separator=" | " 2>/dev/null | \
    sort -t'|' -k1,1 || true
  
  echo ""
  log_success "Notice: Each user's messages in same partition, but users spread across partitions"
}

demo_null_keys() {
  log_demo "Demo 3: Null Keys → Round-Robin Distribution"
  
  # Delete and recreate topic for clean slate
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  sleep 2
  create_topic >/dev/null 2>&1
  
  echo "Sending messages without keys (null keys)..."
  
  for i in {1..15}; do
    echo "Log message $i without key"
  done | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    2>/dev/null
  
  sleep 2
  
  log_info "Showing partition distribution..."
  
  local output
  output=$(timeout 3 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --from-beginning \
    --property print.partition=true \
    2>/dev/null || true)
  
  echo "$output"
  
  echo ""
  echo "Partition distribution:"
  echo "$output" | awk '{print $1}' | sort | uniq -c | sort -k2
  
  echo ""
  log_success "Notice: Messages distributed across partitions (round-robin)"
}

demo_ordering_within_partition() {
  log_demo "Demo 4: Ordering Guarantee Within Partition"
  
  # Clean start
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  sleep 2
  create_topic >/dev/null 2>&1
  
  echo "Sending ordered sequence for user-X..."
  
  for i in {1..10}; do
    echo "user-X:Order-$i"
    sleep 0.1  # Small delay to ensure order
  done | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --property "parse.key=true" \
    --property "key.separator=:" \
    2>/dev/null
  
  sleep 2
  
  log_info "Consuming messages to verify order..."
  
  timeout 3 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --from-beginning \
    --property print.key=true \
    --property print.offset=true \
    --property print.partition=true \
    --property key.separator=" | " 2>/dev/null || true
  
  echo ""
  log_success "Notice: Messages consumed in exact order they were sent (within partition)"
}

demo_partition_assignment() {
  log_demo "Demo 5: Understanding Partition Assignment"
  
  # Clean start
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  sleep 2
  create_topic >/dev/null 2>&1
  
  echo "Testing which partition each key goes to..."
  echo ""
  
  # Test multiple keys
  declare -A key_to_partition
  
  for key in user-{1..20}; do
    # Send one message with this key
    echo "${key}:test" | kafka-console-producer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$TOPIC_NAME" \
      --property "parse.key=true" \
      --property "key.separator=:" \
      2>/dev/null
    
    sleep 0.2
    
    # Find which partition it went to
    local partition
    partition=$(timeout 2 kafka-console-consumer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$TOPIC_NAME" \
      --from-beginning \
      --max-messages 1 \
      --property print.partition=true \
      2>/dev/null | tail -1 | awk '{print $1}' || echo "?")
    
    key_to_partition[$key]=$partition
    
    # Delete topic to reset for next key
    kafka-topics.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --delete --topic "$TOPIC_NAME" \
      --if-exists 2>/dev/null || true
    sleep 1
    create_topic >/dev/null 2>&1
  done
  
  echo "Key → Partition Mapping:"
  echo "─────────────────────────"
  
  # Group by partition
  for partition in $(seq 0 $((NUM_PARTITIONS - 1))); do
    echo "Partition $partition:"
    for key in "${!key_to_partition[@]}"; do
      if [ "${key_to_partition[$key]}" = "$partition" ]; then
        echo "  • $key"
      fi
    done | sort
  done
  
  echo ""
  log_success "Notice: Same key always goes to same partition (deterministic)"
}

analyze_distribution() {
  log_demo "Demo 6: Key Distribution Analysis"
  
  # Clean start
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  sleep 2
  create_topic >/dev/null 2>&1
  
  echo "Sending 100 messages with diverse keys..."
  
  for i in {1..100}; do
    local user=$((i % 20 + 1))  # 20 unique users
    echo "user-${user}:Message $i"
  done | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --property "parse.key=true" \
    --property "key.separator=:" \
    2>/dev/null
  
  sleep 3
  
  log_info "Analyzing partition distribution..."
  
  local output
  output=$(timeout 5 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --from-beginning \
    --property print.partition=true \
    2>/dev/null || true)
  
  echo ""
  echo "Messages per partition:"
  echo "$output" | awk '{print $1}' | sort | uniq -c | \
    awk '{printf "  Partition %s: %3d messages (%.1f%%)\n", $2, $1, $1}'
  
  echo ""
  echo "Distribution quality:"
  local total=$(echo "$output" | wc -l | tr -d ' ')
  local expected=$((total / NUM_PARTITIONS))
  echo "  Total messages: $total"
  echo "  Expected per partition: ~$expected"
  echo "  Actual distribution: See above"
  
  echo ""
  log_success "Notice: Hash-based partitioning provides roughly even distribution"
}

cleanup_topic() {
  log_info "Cleaning up demo topic"
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
}

main() {
  log_section "Kafka Partitioning Demonstration"
  
  log_info "Configuration:"
  echo "  Topic:        $TOPIC_NAME"
  echo "  Partitions:   $NUM_PARTITIONS"
  echo "  Bootstrap:    $KAFKA_BOOTSTRAP_SERVERS"
  
  # Setup
  create_topic
  
  # Run demos
  demo_same_key_same_partition
  demo_different_keys
  demo_null_keys
  demo_ordering_within_partition
  demo_partition_assignment
  analyze_distribution
  
  # Cleanup
  cleanup_topic
  
  log_section "Demonstration Complete"
  log_success "All partitioning concepts demonstrated"
  
  echo ""
  echo "Key Takeaways:"
  echo "  1. Same key always goes to same partition (deterministic)"
  echo "  2. Different keys are distributed across partitions"
  echo "  3. Null keys are distributed round-robin"
  echo "  4. Ordering is guaranteed within a partition only"
  echo "  5. Hash-based partitioning provides even distribution"
  echo ""
}

trap cleanup_topic EXIT

main "$@"
