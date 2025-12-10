#!/usr/bin/env bash
#
# benchmark_throughput.sh
# 
# Benchmark Kafka producer throughput with different configurations.
# Tests batching, compression, and linger settings impact on performance.
#
# Usage:
#   ./benchmark_throughput.sh [topic_name] [num_messages]
#
# Examples:
#   ./benchmark_throughput.sh                          # Use defaults
#   ./benchmark_throughput.sh perf-test 50000         # Custom topic and count
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
TOPIC_NAME="${1:-perf-benchmark-topic}"
NUM_MESSAGES="${2:-10000}"
RECORD_SIZE=1000
PARTITIONS=6
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
    log_error "This tool is included with Apache Kafka"
    exit 1
  fi
}

create_test_topic() {
  log_info "Creating test topic: $TOPIC_NAME"
  
  # Delete if exists
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  
  # Create fresh
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --if-not-exists
  
  sleep 2
  log_success "Topic created: $TOPIC_NAME ($PARTITIONS partitions)"
}

run_benchmark() {
  local test_name="$1"
  local props="$2"
  
  log_test "Test: $test_name"
  
  local start_time=$(date +%s)
  
  # Run performance test
  local output
  output=$(kafka-producer-perf-test.sh \
    --topic "$TOPIC_NAME" \
    --num-records "$NUM_MESSAGES" \
    --record-size "$RECORD_SIZE" \
    --throughput -1 \
    --producer-props \
      bootstrap.servers="$KAFKA_BOOTSTRAP_SERVERS" \
      $props 2>&1)
  
  local end_time=$(date +%s)
  local duration=$((end_time - start_time))
  
  # Parse results
  local throughput=$(echo "$output" | grep -oP '\d+\.\d+ records/sec' | head -1 || echo "N/A")
  local mb_sec=$(echo "$output" | grep -oP '\d+\.\d+ MB/sec' | head -1 || echo "N/A")
  local avg_latency=$(echo "$output" | grep -oP 'avg latency \d+\.\d+ ms' | grep -oP '\d+\.\d+' || echo "N/A")
  local max_latency=$(echo "$output" | grep -oP 'max latency \d+ ms' | grep -oP '\d+' || echo "N/A")
  
  # Display results
  echo "  Duration:     ${duration}s"
  echo "  Throughput:   $throughput"
  echo "  Bandwidth:    $mb_sec"
  echo "  Avg Latency:  ${avg_latency} ms"
  echo "  Max Latency:  ${max_latency} ms"
  
  # Store for comparison
  echo "$test_name|$throughput|$mb_sec|${avg_latency}|${max_latency}" >> /tmp/benchmark_results.txt
}

cleanup_topic() {
  log_info "Cleaning up test topic"
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
}

display_summary() {
  log_section "BENCHMARK SUMMARY"
  
  if [ ! -f /tmp/benchmark_results.txt ]; then
    log_error "No results found"
    return
  fi
  
  echo ""
  printf "%-30s | %-20s | %-15s | %-12s | %-12s\n" \
    "Test Configuration" "Throughput" "Bandwidth" "Avg Latency" "Max Latency"
  echo "─────────────────────────────────────────────────────────────────────────────────────────────────"
  
  while IFS='|' read -r name throughput bandwidth avg_lat max_lat; do
    printf "%-30s | %-20s | %-15s | %-12s | %-12s\n" \
      "$name" "$throughput" "$bandwidth" "${avg_lat} ms" "${max_lat} ms"
  done < /tmp/benchmark_results.txt
  
  echo ""
  
  # Cleanup results file
  rm -f /tmp/benchmark_results.txt
}

main() {
  log_section "Kafka Producer Performance Benchmark"
  
  log_info "Configuration:"
  echo "  Topic:           $TOPIC_NAME"
  echo "  Messages:        $NUM_MESSAGES"
  echo "  Record size:     $RECORD_SIZE bytes"
  echo "  Partitions:      $PARTITIONS"
  echo "  Bootstrap:       $KAFKA_BOOTSTRAP_SERVERS"
  
  # Check prerequisites
  check_perf_tool
  
  # Setup
  create_test_topic
  
  # Clear previous results
  rm -f /tmp/benchmark_results.txt
  
  log_section "Running Benchmarks"
  
  # Test 1: Baseline (defaults)
  run_benchmark "Baseline (defaults)" \
    "acks=1 compression.type=none batch.size=16384 linger.ms=0"
  
  # Test 2: With compression (LZ4)
  run_benchmark "With LZ4 compression" \
    "acks=1 compression.type=lz4 batch.size=16384 linger.ms=0"
  
  # Test 3: Larger batches
  run_benchmark "Large batches (64KB)" \
    "acks=1 compression.type=lz4 batch.size=65536 linger.ms=0"
  
  # Test 4: With linger
  run_benchmark "With linger (10ms)" \
    "acks=1 compression.type=lz4 batch.size=32768 linger.ms=10"
  
  # Test 5: Optimized throughput
  run_benchmark "Optimized (batch+linger)" \
    "acks=1 compression.type=lz4 batch.size=65536 linger.ms=20"
  
  # Test 6: With reliability (acks=all)
  run_benchmark "Reliable (acks=all)" \
    "acks=all enable.idempotence=true compression.type=lz4 batch.size=32768 linger.ms=10"
  
  # Test 7: Maximum throughput
  run_benchmark "Max throughput (zstd)" \
    "acks=1 compression.type=zstd batch.size=131072 linger.ms=100"
  
  # Display summary
  display_summary
  
  # Cleanup
  cleanup_topic
  
  log_section "Benchmark Complete"
  log_success "All tests completed successfully"
  
  echo ""
  echo "Key Takeaways:"
  echo "  • Compression significantly improves throughput"
  echo "  • Larger batches reduce overhead"
  echo "  • Small linger.ms (10-20ms) helps without much latency cost"
  echo "  • acks=all adds reliability with moderate throughput impact"
  echo ""
}

# Trap errors and cleanup
trap cleanup_topic EXIT

main "$@"
