#!/usr/bin/env bash
#
# compare_sizes.sh
#
# Compare message sizes and throughput across different serialization formats.
# Demonstrates the impact of format choice on bandwidth and storage.
#
# Usage:
#   ./compare_sizes.sh [num_messages]
#
# Examples:
#   ./compare_sizes.sh           # Default: 1000 messages
#   ./compare_sizes.sh 5000      # Custom message count
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
NUM_MESSAGES="${1:-1000}"
TOPIC_PREFIX="size-comparison"

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

generate_string_messages() {
  local count=$1
  for i in $(seq 1 "$count"); do
    echo "user-$i,purchase,$((RANDOM % 1000 + 1)).$((RANDOM % 100)),$(date +%s)000"
  done
}

generate_json_compact_messages() {
  local count=$1
  for i in $(seq 1 "$count"); do
    echo "{\"userId\":\"user-$i\",\"eventType\":\"purchase\",\"amount\":$((RANDOM % 1000 + 1)).$((RANDOM % 100)),\"timestamp\":$(date +%s)000}"
  done
}

generate_json_formatted_messages() {
  local count=$1
  for i in $(seq 1 "$count"); do
    cat << EOF
{
  "userId": "user-$i",
  "eventType": "purchase",
  "amount": $((RANDOM % 1000 + 1)).$((RANDOM % 100)),
  "timestamp": $(date +%s)000,
  "metadata": {
    "source": "web",
    "version": "2.0"
  }
}
EOF
  done
}

test_string_format() {
  log_test "Test 1: String/CSV Format"
  
  local topic="${TOPIC_PREFIX}-string"
  
  # Create topic
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$topic" \
    --if-exists 2>/dev/null || true
  sleep 1
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create --topic "$topic" \
    --partitions 3 --replication-factor 1 2>/dev/null
  sleep 1
  
  echo "Generating and sending $NUM_MESSAGES CSV messages..."
  
  # Generate and send
  local start_time=$(date +%s)
  generate_string_messages "$NUM_MESSAGES" | \
    kafka-console-producer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$topic" \
      2>/dev/null
  local end_time=$(date +%s)
  local duration=$((end_time - start_time))
  
  # Calculate size
  sleep 2
  local size=$(get_topic_size "$topic")
  local avg_msg_size=$(echo "scale=2; $size / $NUM_MESSAGES" | bc)
  
  echo "Results:"
  echo "  Messages sent:    $NUM_MESSAGES"
  echo "  Total size:       $(format_bytes $size)"
  echo "  Avg message size: ${avg_msg_size} bytes"
  echo "  Duration:         ${duration}s"
  echo "  Throughput:       $((NUM_MESSAGES / (duration + 1))) msg/s"
  
  echo "$topic|$size|$avg_msg_size|$duration" >> /tmp/size_comparison.txt
}

test_json_compact() {
  log_test "Test 2: JSON (Compact)"
  
  local topic="${TOPIC_PREFIX}-json-compact"
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$topic" \
    --if-exists 2>/dev/null || true
  sleep 1
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create --topic "$topic" \
    --partitions 3 --replication-factor 1 2>/dev/null
  sleep 1
  
  echo "Generating and sending $NUM_MESSAGES compact JSON messages..."
  
  local start_time=$(date +%s)
  generate_json_compact_messages "$NUM_MESSAGES" | \
    kafka-console-producer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$topic" \
      2>/dev/null
  local end_time=$(date +%s)
  local duration=$((end_time - start_time))
  
  sleep 2
  local size=$(get_topic_size "$topic")
  local avg_msg_size=$(echo "scale=2; $size / $NUM_MESSAGES" | bc)
  
  echo "Results:"
  echo "  Messages sent:    $NUM_MESSAGES"
  echo "  Total size:       $(format_bytes $size)"
  echo "  Avg message size: ${avg_msg_size} bytes"
  echo "  Duration:         ${duration}s"
  echo "  Throughput:       $((NUM_MESSAGES / (duration + 1))) msg/s"
  
  echo "$topic|$size|$avg_msg_size|$duration" >> /tmp/size_comparison.txt
}

test_json_formatted() {
  log_test "Test 3: JSON (Formatted/Pretty)"
  
  local topic="${TOPIC_PREFIX}-json-formatted"
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$topic" \
    --if-exists 2>/dev/null || true
  sleep 1
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create --topic "$topic" \
    --partitions 3 --replication-factor 1 2>/dev/null
  sleep 1
  
  echo "Generating and sending $NUM_MESSAGES formatted JSON messages..."
  
  local start_time=$(date +%s)
  generate_json_formatted_messages "$NUM_MESSAGES" | \
    kafka-console-producer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$topic" \
      2>/dev/null
  local end_time=$(date +%s)
  local duration=$((end_time - start_time))
  
  sleep 2
  local size=$(get_topic_size "$topic")
  local avg_msg_size=$(echo "scale=2; $size / $NUM_MESSAGES" | bc)
  
  echo "Results:"
  echo "  Messages sent:    $NUM_MESSAGES"
  echo "  Total size:       $(format_bytes $size)"
  echo "  Avg message size: ${avg_msg_size} bytes"
  echo "  Duration:         ${duration}s"
  echo "  Throughput:       $((NUM_MESSAGES / (duration + 1))) msg/s"
  
  echo "$topic|$size|$avg_msg_size|$duration" >> /tmp/size_comparison.txt
}

get_topic_size() {
  local topic=$1
  
  # Get total size from kafka-log-dirs
  local size=$(kafka-log-dirs.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic-list "$topic" \
    --describe 2>/dev/null | \
    grep -oP '"size":\d+' | \
    grep -oP '\d+' | \
    awk '{sum+=$1} END {print sum}' || echo "0")
  
  echo "${size:-0}"
}

format_bytes() {
  local bytes=$1
  if [ "$bytes" -ge 1073741824 ]; then
    echo "$(echo "scale=2; $bytes / 1073741824" | bc) GB"
  elif [ "$bytes" -ge 1048576 ]; then
    echo "$(echo "scale=2; $bytes / 1048576" | bc) MB"
  elif [ "$bytes" -ge 1024 ]; then
    echo "$(echo "scale=2; $bytes / 1024" | bc) KB"
  else
    echo "$bytes bytes"
  fi
}

display_summary() {
  log_section "Comparison Summary"
  
  if [ ! -f /tmp/size_comparison.txt ]; then
    log_error "No comparison data found"
    return
  fi
  
  echo ""
  echo "Message Size Comparison (for $NUM_MESSAGES messages):"
  echo "══════════════════════════════════════════════════════════════════"
  
  printf "%-25s | %15s | %15s | %10s\n" "Format" "Total Size" "Avg Msg Size" "Duration"
  echo "──────────────────────────────────────────────────────────────────"
  
  local baseline_size=0
  local first=true
  
  while IFS='|' read -r topic size avg_size duration; do
    local format_name=$(echo "$topic" | sed "s/${TOPIC_PREFIX}-//")
    local formatted_size=$(format_bytes "$size")
    
    if $first; then
      baseline_size=$size
      first=false
      printf "%-25s | %15s | %15.2f | %8ss (baseline)\n" \
        "$format_name" "$formatted_size" "$avg_size" "$duration"
    else
      local ratio=$(echo "scale=2; $size * 100 / $baseline_size" | bc)
      printf "%-25s | %15s | %15.2f | %8ss (%d%% of baseline)\n" \
        "$format_name" "$formatted_size" "$avg_size" "$duration" "${ratio%.*}"
    fi
  done < /tmp/size_comparison.txt
  
  echo ""
  
  # Calculate projected costs
  echo "Projected Daily Volume (at 1 million messages/day):"
  echo "──────────────────────────────────────────────────────────────────"
  
  while IFS='|' read -r topic size avg_size duration; do
    local format_name=$(echo "$topic" | sed "s/${TOPIC_PREFIX}-//")
    local daily_size=$((size * 1000000 / NUM_MESSAGES))
    local daily_formatted=$(format_bytes "$daily_size")
    
    printf "%-25s: %15s/day\n" "$format_name" "$daily_formatted"
  done < /tmp/size_comparison.txt
  
  echo ""
  
  # Recommendations
  echo "Recommendations:"
  echo "  • For debugging/development: JSON compact"
  echo "  • For production/scale: Binary formats (Avro/Protobuf)"
  echo "  • Avoid formatted JSON in production (wastes bandwidth)"
  echo "  • Use compression (lz4/zstd) with any format"
  echo ""
  echo "Note: Binary formats (Avro/Protobuf) typically 50-70% smaller"
  echo "      than JSON, but require Schema Registry setup."
}

cleanup() {
  log_info "Cleaning up test topics"
  
  for topic in string json-compact json-formatted; do
    kafka-topics.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --delete --topic "${TOPIC_PREFIX}-${topic}" \
      --if-exists 2>/dev/null || true
  done
  
  rm -f /tmp/size_comparison.txt
}

main() {
  log_section "Message Size Comparison"
  
  log_info "Configuration:"
  echo "  Messages:     $NUM_MESSAGES"
  echo "  Topic prefix: $TOPIC_PREFIX"
  echo "  Bootstrap:    $KAFKA_BOOTSTRAP_SERVERS"
  
  # Clear previous results
  rm -f /tmp/size_comparison.txt
  
  # Run tests
  test_string_format
  test_json_compact
  test_json_formatted
  
  # Show summary
  display_summary
  
  # Cleanup
  cleanup
  
  log_section "Comparison Complete"
  log_success "All format comparisons completed"
  
  echo ""
  echo "Key Insights:"
  echo "  1. JSON formatted is significantly larger (whitespace overhead)"
  echo "  2. Compact JSON is better but still verbose"
  echo "  3. String/CSV is smallest text format (no field names)"
  echo "  4. Binary formats (not tested) would be ~50-70% smaller"
  echo "  5. Compression helps all formats significantly"
  echo ""
}

trap cleanup EXIT

main "$@"
