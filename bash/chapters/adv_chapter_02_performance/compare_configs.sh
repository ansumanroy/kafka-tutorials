#!/usr/bin/env bash
#
# compare_configs.sh
#
# Compare different compression algorithms and their impact on performance.
# Demonstrates the tradeoffs between compression ratio, CPU usage, and throughput.
#
# Usage:
#   ./compare_configs.sh [topic_name] [num_messages]
#
# Examples:
#   ./compare_configs.sh                              # Use defaults
#   ./compare_configs.sh compress-test 20000         # Custom parameters
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
TOPIC_NAME="${1:-compression-compare-topic}"
NUM_MESSAGES="${2:-5000}"
RECORD_SIZE=1000
PARTITIONS=3
REPLICATION_FACTOR=1

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

check_perf_tool() {
  if ! command -v kafka-producer-perf-test.sh &> /dev/null; then
    log_error "kafka-producer-perf-test.sh not found in PATH"
    exit 1
  fi
}

create_test_topic() {
  log_info "Creating test topic: $TOPIC_NAME"
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --if-not-exists
  
  sleep 2
  log_success "Topic ready"
}

test_compression() {
  local compression_type="$1"
  local display_name="$2"
  
  log_test "Testing: $display_name"
  
  local start_time=$(date +%s.%N)
  
  local output
  output=$(kafka-producer-perf-test.sh \
    --topic "$TOPIC_NAME" \
    --num-records "$NUM_MESSAGES" \
    --record-size "$RECORD_SIZE" \
    --throughput -1 \
    --producer-props \
      bootstrap.servers="$KAFKA_BOOTSTRAP_SERVERS" \
      compression.type="$compression_type" \
      batch.size=32768 \
      linger.ms=10 \
      acks=1 2>&1)
  
  local end_time=$(date +%s.%N)
  local duration=$(echo "$end_time - $start_time" | bc)
  
  # Parse output
  local throughput=$(echo "$output" | grep -oP '\d+\.\d+ records/sec' | head -1)
  local mb_sec=$(echo "$output" | grep -oP '\d+\.\d+ MB/sec' | head -1)
  local avg_latency=$(echo "$output" | grep -oP 'avg latency \d+\.\d+ ms' | grep -oP '\d+\.\d+')
  
  # Get topic size after compression
  local topic_size=$(kafka-log-dirs.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic-list "$TOPIC_NAME" \
    --describe 2>/dev/null | grep -oP '"size":\d+' | grep -oP '\d+' | awk '{sum+=$1} END {print sum}' || echo "0")
  
  local topic_size_mb=$(echo "scale=2; $topic_size / 1024 / 1024" | bc)
  
  # Calculate compression ratio (uncompressed would be NUM_MESSAGES * RECORD_SIZE)
  local uncompressed_mb=$(echo "scale=2; $NUM_MESSAGES * $RECORD_SIZE / 1024 / 1024" | bc)
  local ratio=$(echo "scale=2; $uncompressed_mb / $topic_size_mb" | bc 2>/dev/null || echo "1.00")
  
  echo "  Throughput:       $throughput"
  echo "  Bandwidth:        $mb_sec"
  echo "  Avg Latency:      ${avg_latency} ms"
  echo "  Duration:         ${duration}s"
  echo "  Disk Size:        ${topic_size_mb} MB"
  echo "  Compression:      ${ratio}x"
  
  # Store results
  echo "$display_name|$compression_type|$throughput|$mb_sec|${avg_latency}|${duration}|${topic_size_mb}|${ratio}" \
    >> /tmp/compression_results.txt
  
  # Clear topic data for next test
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" 2>/dev/null || true
  sleep 2
  create_test_topic
}

test_batch_sizes() {
  log_section "Batch Size Comparison"
  
  local batch_sizes=(8192 16384 32768 65536 131072)
  
  for size in "${batch_sizes[@]}"; do
    local size_kb=$((size / 1024))
    log_test "Batch Size: ${size_kb}KB"
    
    local output
    output=$(kafka-producer-perf-test.sh \
      --topic "$TOPIC_NAME" \
      --num-records "$NUM_MESSAGES" \
      --record-size "$RECORD_SIZE" \
      --throughput -1 \
      --producer-props \
        bootstrap.servers="$KAFKA_BOOTSTRAP_SERVERS" \
        compression.type=lz4 \
        batch.size="$size" \
        linger.ms=10 \
        acks=1 2>&1)
    
    local throughput=$(echo "$output" | grep -oP '\d+\.\d+ records/sec' | head -1)
    local avg_latency=$(echo "$output" | grep -oP 'avg latency \d+\.\d+ ms' | grep -oP '\d+\.\d+')
    
    echo "  Throughput:   $throughput"
    echo "  Avg Latency:  ${avg_latency} ms"
    
    echo "${size_kb}KB|$throughput|${avg_latency}" >> /tmp/batch_results.txt
  done
}

test_linger_settings() {
  log_section "Linger Time Comparison"
  
  local linger_values=(0 5 10 20 50 100)
  
  for linger in "${linger_values[@]}"; do
    log_test "Linger Time: ${linger}ms"
    
    local output
    output=$(kafka-producer-perf-test.sh \
      --topic "$TOPIC_NAME" \
      --num-records "$NUM_MESSAGES" \
      --record-size "$RECORD_SIZE" \
      --throughput -1 \
      --producer-props \
        bootstrap.servers="$KAFKA_BOOTSTRAP_SERVERS" \
        compression.type=lz4 \
        batch.size=32768 \
        linger.ms="$linger" \
        acks=1 2>&1)
    
    local throughput=$(echo "$output" | grep -oP '\d+\.\d+ records/sec' | head -1)
    local avg_latency=$(echo "$output" | grep -oP 'avg latency \d+\.\d+ ms' | grep -oP '\d+\.\d+')
    
    echo "  Throughput:   $throughput"
    echo "  Avg Latency:  ${avg_latency} ms"
    
    echo "${linger}ms|$throughput|${avg_latency}" >> /tmp/linger_results.txt
  done
}

display_compression_summary() {
  log_section "COMPRESSION COMPARISON SUMMARY"
  
  if [ ! -f /tmp/compression_results.txt ]; then
    log_error "No compression results found"
    return
  fi
  
  echo ""
  printf "%-15s | %-20s | %-15s | %-12s | %-10s | %-12s\n" \
    "Algorithm" "Throughput" "Bandwidth" "Latency" "Disk Size" "Ratio"
  echo "────────────────────────────────────────────────────────────────────────────────────────────────"
  
  while IFS='|' read -r name type throughput bandwidth latency duration size ratio; do
    printf "%-15s | %-20s | %-15s | %-12s | %-10s | %-12s\n" \
      "$name" "$throughput" "$bandwidth" "${latency} ms" "${size} MB" "${ratio}x"
  done < /tmp/compression_results.txt
  
  echo ""
}

display_batch_summary() {
  log_section "BATCH SIZE COMPARISON SUMMARY"
  
  if [ ! -f /tmp/batch_results.txt ]; then
    return
  fi
  
  echo ""
  printf "%-15s | %-25s | %-15s\n" "Batch Size" "Throughput" "Avg Latency"
  echo "───────────────────────────────────────────────────────────"
  
  while IFS='|' read -r size throughput latency; do
    printf "%-15s | %-25s | %-15s\n" "$size" "$throughput" "${latency} ms"
  done < /tmp/batch_results.txt
  
  echo ""
}

display_linger_summary() {
  log_section "LINGER TIME COMPARISON SUMMARY"
  
  if [ ! -f /tmp/linger_results.txt ]; then
    return
  fi
  
  echo ""
  printf "%-15s | %-25s | %-15s\n" "Linger Time" "Throughput" "Avg Latency"
  echo "───────────────────────────────────────────────────────────"
  
  while IFS='|' read -r linger throughput latency; do
    printf "%-15s | %-25s | %-15s\n" "$linger" "$throughput" "${latency} ms"
  done < /tmp/linger_results.txt
  
  echo ""
}

cleanup_topic() {
  log_info "Cleaning up test topic"
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
}

main() {
  log_section "Kafka Producer Configuration Comparison"
  
  log_info "Test parameters:"
  echo "  Topic:        $TOPIC_NAME"
  echo "  Messages:     $NUM_MESSAGES"
  echo "  Record size:  $RECORD_SIZE bytes"
  echo "  Partitions:   $PARTITIONS"
  
  check_perf_tool
  create_test_topic
  
  # Clear previous results
  rm -f /tmp/compression_results.txt /tmp/batch_results.txt /tmp/linger_results.txt
  
  # Test 1: Compression Algorithms
  log_section "Testing Compression Algorithms"
  
  test_compression "none" "No Compression"
  test_compression "snappy" "Snappy"
  test_compression "lz4" "LZ4"
  test_compression "zstd" "ZSTD"
  test_compression "gzip" "GZIP"
  
  display_compression_summary
  
  # Test 2: Batch Sizes
  test_batch_sizes
  display_batch_summary
  
  # Test 3: Linger Settings
  test_linger_settings
  display_linger_summary
  
  # Cleanup
  cleanup_topic
  rm -f /tmp/compression_results.txt /tmp/batch_results.txt /tmp/linger_results.txt
  
  log_section "Comparison Complete"
  log_success "All configuration tests completed"
  
  echo ""
  echo "Recommendations based on results:"
  echo "  • For general use: LZ4 compression with 32KB batches"
  echo "  • For max throughput: ZSTD compression with 64KB+ batches and 20ms linger"
  echo "  • For low latency: Snappy compression with 16KB batches and 0ms linger"
  echo "  • For high compression: GZIP or ZSTD (slower but better ratio)"
  echo ""
}

trap cleanup_topic EXIT

main "$@"
