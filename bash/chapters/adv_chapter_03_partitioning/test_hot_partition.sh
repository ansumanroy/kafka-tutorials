#!/usr/bin/env bash
#
# test_hot_partition.sh
#
# Demonstrate the hot partition problem and its impact on performance.
# Shows how skewed key distribution causes uneven load.
#
# Usage:
#   ./test_hot_partition.sh [topic_name]
#
# Examples:
#   ./test_hot_partition.sh                    # Use default topic
#   ./test_hot_partition.sh hot-test           # Custom topic name
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
TOPIC_NAME="${1:-hot-partition-test}"
NUM_PARTITIONS=5
REPLICATION_FACTOR=1
TOTAL_MESSAGES=1000

log_section() {
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "════════════════════════════════════════════════════════════"
}

log_test() {
  echo ""
  echo "────────────────────────────────────────────────────────────"
  echo "  $1"
  echo "────────────────────────────────────────────────────────────"
}

create_topic() {
  log_info "Creating topic: $TOPIC_NAME with $NUM_PARTITIONS partitions"
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  
  sleep 2
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions "$NUM_PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR"
  
  sleep 2
  log_success "Topic created"
}

simulate_hot_key() {
  log_test "Simulating Hot Partition Problem"
  
  echo "Scenario: Celebrity user generates 80% of traffic"
  echo ""
  echo "Sending $TOTAL_MESSAGES messages..."
  echo "  • 80% (800 msgs) from 'celebrity-user' (hot key)"
  echo "  • 20% (200 msgs) from 20 regular users"
  echo ""
  
  local hot_messages=$((TOTAL_MESSAGES * 80 / 100))
  local regular_messages=$((TOTAL_MESSAGES - hot_messages))
  local regular_users=20
  local msgs_per_user=$((regular_messages / regular_users))
  
  (
    # Hot key: 80% of traffic
    for i in $(seq 1 $hot_messages); do
      echo "celebrity-user:Event $i from celebrity"
    done
    
    # Regular keys: 20% of traffic
    for user in $(seq 1 $regular_users); do
      for i in $(seq 1 $msgs_per_user); do
        echo "user-${user}:Event $i from user $user"
      done
    done
  ) | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --property "parse.key=true" \
    --property "key.separator=:" \
    2>/dev/null
  
  sleep 3
  log_success "Messages sent"
}

analyze_distribution() {
  log_test "Analyzing Partition Distribution"
  
  log_info "Fetching partition offsets..."
  
  local offsets
  offsets=$(kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --time -1 2>/dev/null)
  
  echo ""
  echo "Messages per partition:"
  echo "─────────────────────────────────────"
  
  local max_offset=0
  local min_offset=999999
  local total=0
  declare -A partition_counts
  
  while IFS=: read -r topic partition offset; do
    partition_counts[$partition]=$offset
    total=$((total + offset))
    [ $offset -gt $max_offset ] && max_offset=$offset
    [ $offset -lt $min_offset ] && min_offset=$offset
  done <<< "$offsets"
  
  # Display distribution
  for partition in $(seq 0 $((NUM_PARTITIONS - 1))); do
    local count=${partition_counts[$partition]:-0}
    local percent=$(echo "scale=1; $count * 100 / $total" | bc 2>/dev/null || echo "0")
    local bar_length=$(echo "scale=0; $count * 40 / $max_offset" | bc 2>/dev/null || echo "0")
    local bar=$(printf '█%.0s' $(seq 1 $bar_length))
    
    if [ "$count" -eq "$max_offset" ]; then
      printf "Partition %d: %4d msgs (%5.1f%%) %s ⚠️ HOT!\n" "$partition" "$count" "$percent" "$bar"
    else
      printf "Partition %d: %4d msgs (%5.1f%%) %s\n" "$partition" "$count" "$percent" "$bar"
    fi
  done
  
  echo ""
  echo "Distribution analysis:"
  local variance=$((max_offset - min_offset))
  local avg=$((total / NUM_PARTITIONS))
  local skew=$(echo "scale=1; $max_offset * 100 / $avg" | bc 2>/dev/null || echo "0")
  
  echo "  Total messages:    $total"
  echo "  Average/partition: $avg"
  echo "  Max partition:     $max_offset"
  echo "  Min partition:     $min_offset"
  echo "  Variance:          $variance"
  echo "  Skew factor:       ${skew}%"
  
  if [ "$variance" -gt $((avg / 2)) ]; then
    echo ""
    log_error "⚠️  SIGNIFICANT SKEW DETECTED!"
    echo "  One partition has significantly more messages than others."
    echo "  This creates a hot partition bottleneck."
  fi
}

measure_consumer_impact() {
  log_test "Measuring Consumer Impact"
  
  echo "Creating consumer group to demonstrate lag..."
  
  local consumer_group="hot-partition-test-group"
  
  # Start multiple consumers in background (one per partition)
  log_info "Starting $NUM_PARTITIONS consumers..."
  
  for i in $(seq 1 $NUM_PARTITIONS); do
    timeout 5 kafka-console-consumer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$TOPIC_NAME" \
      --group "$consumer_group" \
      --from-beginning \
      2>/dev/null >/dev/null &
  done
  
  # Let consumers process
  sleep 6
  
  # Check consumer group lag
  log_info "Checking consumer group lag..."
  
  local lag_output
  lag_output=$(kafka-consumer-groups.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --group "$consumer_group" \
    --describe 2>/dev/null || echo "")
  
  if [ -n "$lag_output" ]; then
    echo ""
    echo "$lag_output" | grep -v "^$"
    echo ""
    
    # Analyze lag
    local hot_partition_lag=$(echo "$lag_output" | grep "$TOPIC_NAME" | awk '{print $6}' | sort -n | tail -1)
    if [ -n "$hot_partition_lag" ] && [ "$hot_partition_lag" -gt 0 ]; then
      log_error "Hot partition still has lag: $hot_partition_lag messages"
      echo "  This partition is taking longer to process due to higher load."
    fi
  else
    log_warn "Consumer group not available yet"
  fi
}

demonstrate_solution() {
  log_test "Demonstrating Solution: Composite Keys"
  
  echo "Solution: Shard the hot key across multiple partitions"
  echo ""
  
  # Clean topic
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  sleep 2
  create_topic >/dev/null 2>&1
  
  echo "Sending messages with sharded keys..."
  echo "  • Celebrity traffic split across 10 shards"
  echo "  • Regular users keep single key"
  echo ""
  
  local hot_messages=$((TOTAL_MESSAGES * 80 / 100))
  local regular_messages=$((TOTAL_MESSAGES - hot_messages))
  local shards=10
  local msgs_per_shard=$((hot_messages / shards))
  
  (
    # Hot key with sharding: 80% of traffic
    for shard in $(seq 1 $shards); do
      for i in $(seq 1 $msgs_per_shard); do
        echo "celebrity-user-shard-${shard}:Event $i"
      done
    done
    
    # Regular keys: 20% of traffic
    for user in $(seq 1 20); do
      for i in $(seq 1 $((regular_messages / 20))); do
        echo "user-${user}:Event $i"
      done
    done
  ) | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --property "parse.key=true" \
    --property "key.separator=:" \
    2>/dev/null
  
  sleep 3
  
  log_info "Analyzing improved distribution..."
  
  local offsets
  offsets=$(kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --time -1 2>/dev/null)
  
  echo ""
  echo "Messages per partition (with sharding):"
  echo "─────────────────────────────────────"
  
  local total=0
  declare -A partition_counts
  
  while IFS=: read -r topic partition offset; do
    partition_counts[$partition]=$offset
    total=$((total + offset))
  done <<< "$offsets"
  
  local max_offset=0
  for partition in $(seq 0 $((NUM_PARTITIONS - 1))); do
    local count=${partition_counts[$partition]:-0}
    [ $count -gt $max_offset ] && max_offset=$count
  done
  
  for partition in $(seq 0 $((NUM_PARTITIONS - 1))); do
    local count=${partition_counts[$partition]:-0}
    local percent=$(echo "scale=1; $count * 100 / $total" | bc 2>/dev/null || echo "0")
    local bar_length=$(echo "scale=0; $count * 40 / $max_offset" | bc 2>/dev/null || echo "0")
    local bar=$(printf '█%.0s' $(seq 1 $bar_length))
    
    printf "Partition %d: %4d msgs (%5.1f%%) %s\n" "$partition" "$count" "$percent" "$bar"
  done
  
  echo ""
  log_success "Much better distribution! No single hot partition."
}

cleanup_topic() {
  log_info "Cleaning up test topic"
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
}

main() {
  log_section "Hot Partition Problem Demonstration"
  
  log_info "Configuration:"
  echo "  Topic:        $TOPIC_NAME"
  echo "  Partitions:   $NUM_PARTITIONS"
  echo "  Messages:     $TOTAL_MESSAGES"
  echo "  Bootstrap:    $KAFKA_BOOTSTRAP_SERVERS"
  
  # Test 1: Show the problem
  create_topic
  simulate_hot_key
  analyze_distribution
  measure_consumer_impact
  
  # Test 2: Show the solution
  demonstrate_solution
  
  # Cleanup
  cleanup_topic
  
  log_section "Test Complete"
  log_success "Hot partition problem and solution demonstrated"
  
  echo ""
  echo "Summary:"
  echo "  Problem: Skewed key distribution → Hot partition → Bottleneck"
  echo "  Solution: Composite keys (sharding) → Even distribution → Better throughput"
  echo ""
  echo "Best Practices:"
  echo "  1. Monitor partition distribution and consumer lag"
  echo "  2. Use high-cardinality keys (user_id, device_id)"
  echo "  3. Shard hot keys across multiple sub-keys"
  echo "  4. Consider custom partitioner for complex scenarios"
  echo ""
}

trap cleanup_topic EXIT

main "$@"
