#!/usr/bin/env bash
#
# test_error_handling.sh
#
# Test various error scenarios and demonstrate error handling strategies.
# Shows timeout errors, message size limits, and retry behavior.
#
# Usage:
#   ./test_error_handling.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
TEST_TOPIC="error-handling-test"

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

create_test_topic() {
  log_info "Creating test topic: $TEST_TOPIC"
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TEST_TOPIC" \
    --if-exists 2>/dev/null || true
  
  sleep 2
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$TEST_TOPIC" \
    --partitions 3 \
    --replication-factor 1
  
  sleep 2
  log_success "Topic created"
}

test_successful_send() {
  log_test "Test 1: Successful Send (Baseline)"
  
  echo "Sending normal message..."
  
  local start_time=$(date +%s.%N 2>/dev/null || date +%s)
  
  if echo "Test message $(date +%s)" | \
    kafka-console-producer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$TEST_TOPIC" \
      --request-required-acks all \
      2>/dev/null; then
    
    local end_time=$(date +%s.%N 2>/dev/null || date +%s)
    local duration=$(echo "$end_time - $start_time" | bc 2>/dev/null || echo "N/A")
    
    log_success "Message sent successfully"
    echo "  Duration: ${duration}s"
    echo "  Exit code: 0"
  else
    log_error "Failed to send message"
    echo "  Exit code: $?"
  fi
  
  echo ""
  echo "Characteristics:"
  echo "  • Message accepted by broker"
  echo "  • Replicated to all ISRs (acks=all)"
  echo "  • Producer received acknowledgement"
  echo "  • No errors or retries"
}

test_timeout_error() {
  log_test "Test 2: Timeout Error (Unrealistic Timeout)"
  
  echo "Sending message with very short timeout (will fail)..."
  echo "Setting request-timeout-ms=1 (1 millisecond)"
  
  local exit_code=0
  echo "Test message" | \
    kafka-console-producer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$TEST_TOPIC" \
      --request-timeout-ms 1 \
      2>&1 | head -20 || exit_code=$?
  
  if [ $exit_code -ne 0 ]; then
    log_warn "⚠️  Message failed as expected (timeout)"
    echo "  Exit code: $exit_code"
  else
    log_info "Message somehow succeeded (network was very fast)"
  fi
  
  echo ""
  echo "Error Type: TimeoutException (Retriable)"
  echo "  • Cause: request.timeout.ms too short"
  echo "  • Producer behavior: Retry up to 'retries' times"
  echo "  • If all retries fail: Exception thrown"
  echo "  • Solution: Increase timeout or check network"
}

test_invalid_topic() {
  log_test "Test 3: Invalid Topic Name (Non-Retriable Error)"
  
  echo "Attempting to send to invalid topic name..."
  local invalid_topic="invalid topic with spaces!"
  
  local exit_code=0
  echo "Test message" | \
    kafka-console-producer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$invalid_topic" \
      2>&1 | head -20 || exit_code=$?
  
  if [ $exit_code -ne 0 ]; then
    log_warn "⚠️  Message failed as expected (invalid topic)"
    echo "  Exit code: $exit_code"
  fi
  
  echo ""
  echo "Error Type: INVALID_TOPIC_EXCEPTION (Non-Retriable)"
  echo "  • Cause: Topic name contains invalid characters"
  echo "  • Producer behavior: Immediate failure, no retry"
  echo "  • Action needed: Fix topic name in configuration"
  echo "  • Should route to DLQ: Yes (if application level)"
}

test_large_message() {
  log_test "Test 4: Message Too Large (Non-Retriable Error)"
  
  echo "Attempting to send very large message..."
  echo "(This may take a moment to generate)"
  
  # Generate 2MB message (typically exceeds broker limit)
  local large_message=$(dd if=/dev/zero bs=1M count=2 2>/dev/null | base64 | tr -d '\n')
  local message_size=$(echo -n "$large_message" | wc -c | tr -d ' ')
  
  echo "Message size: $(echo "scale=2; $message_size / 1024 / 1024" | bc) MB"
  
  local exit_code=0
  echo "$large_message" | \
    kafka-console-producer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$TEST_TOPIC" \
      2>&1 | head -20 || exit_code=$?
  
  if [ $exit_code -ne 0 ]; then
    log_warn "⚠️  Message failed as expected (too large)"
    echo "  Exit code: $exit_code"
  else
    log_info "Message was accepted (broker has high limit)"
  fi
  
  echo ""
  echo "Error Type: RECORD_TOO_LARGE (Non-Retriable)"
  echo "  • Cause: Message size > max.message.bytes (broker config)"
  echo "  • Default broker limit: 1 MB"
  echo "  • Producer behavior: Immediate failure, no retry"
  echo "  • Solutions:"
  echo "    1. Split message into smaller chunks"
  echo "    2. Increase broker max.message.bytes (not recommended)"
  echo "    3. Use external storage + reference in message"
  echo "  • Should route to DLQ: Yes (for investigation)"
}

test_nonexistent_broker() {
  log_test "Test 5: Connection Error (Non-Existent Broker)"
  
  echo "Attempting to connect to non-existent broker..."
  
  local fake_broker="nonexistent-broker:9092"
  
  timeout 10 bash -c "
    echo 'Test message' | \
    kafka-console-producer.sh \
      --bootstrap-server '$fake_broker' \
      --topic '$TEST_TOPIC' \
      --request-timeout-ms 5000 \
      2>&1 | head -20
  " || log_warn "⚠️  Connection failed as expected"
  
  echo ""
  echo "Error Type: NETWORK_EXCEPTION (Retriable)"
  echo "  • Cause: Cannot connect to broker"
  echo "  • Producer behavior: Retry with exponential backoff"
  echo "  • Eventual result: TimeoutException after delivery.timeout.ms"
  echo "  • Solutions:"
  echo "    1. Check broker address/port"
  echo "    2. Verify network connectivity"
  echo "    3. Check firewall rules"
}

test_retry_behavior() {
  log_test "Test 6: Retry Behavior Demonstration"
  
  cat << 'EOF'
Kafka Producer Retry Logic:
───────────────────────────────────────────────────

Configuration:
  retries = 3
  retry.backoff.ms = 100
  delivery.timeout.ms = 10000

Retry Timeline:
──────────────────────────────────────────────────
Time  Action
────────────────────────────────────────────────── 
0ms   Initial send → Error (NOT_LEADER)
100ms Retry #1 (after backoff) → Error
200ms Retry #2 (after backoff) → Error
300ms Retry #3 (after backoff) → Success! ✓

Total time: 300ms (within delivery.timeout)
Result: Message delivered successfully

Scenario 2: All Retries Exhausted
──────────────────────────────────────────────────
0ms   Initial send → Error
100ms Retry #1 → Error
200ms Retry #2 → Error
300ms Retry #3 → Error
Result: Throw exception to application

Retriable Error Types:
  • NOT_LEADER_FOR_PARTITION (leader election)
  • NOT_ENOUGH_REPLICAS (waiting for replicas)
  • NETWORK_EXCEPTION (transient network issue)
  • REQUEST_TIMED_OUT (broker overloaded)

Non-Retriable Error Types:
  • RECORD_TOO_LARGE (message size limit)
  • INVALID_TOPIC_EXCEPTION (bad topic name)
  • AUTHORIZATION_FAILED (ACL issue)
  • CORRUPT_MESSAGE (malformed data)
  • UNSUPPORTED_VERSION (version mismatch)
EOF
}

test_error_logging() {
  log_test "Test 7: Error Logging Best Practices"
  
  cat << 'EOF'
What to Log for Errors:
───────────────────────────────────────────────────

Minimal (Always):
  • Timestamp
  • Error type/code
  • Topic name
  • Message key (NOT value - may contain PII)
  • Retry count

Good (Structured):
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "ERROR",
  "event": "send_failed",
  "topic": "user-events",
  "partition": 2,
  "key": "user-123",
  "error_type": "RECORD_TOO_LARGE",
  "error_message": "Message size exceeds limit",
  "message_size_bytes": 2097152,
  "retry_count": 3,
  "correlation_id": "abc-123"
}

Excellent (With Context):
{
  ...previous fields...
  "producer_id": "api-gateway-producer-1",
  "service": "order-service",
  "host": "api-01.prod.example.com",
  "trace_id": "550e8400-e29b-41d4-a716-446655440000",
  "span_id": "6ba7b810-9dad-11d1",
  "routed_to_dlq": true,
  "dlq_topic": "user-events-dlq"
}

Do NOT Log:
  ✗ Full message value (PII/security)
  ✗ Sensitive data (passwords, tokens)
  ✗ Every successful send (too verbose)
  ✗ Full stack traces for retriable errors
EOF
}

demonstrate_monitoring() {
  log_test "Test 8: Monitoring and Metrics"
  
  cat << 'EOF'
Key Metrics to Monitor:
───────────────────────────────────────────────────

Throughput:
  record-send-rate         Current: 15,000 msg/s
  byte-rate                Current: 15 MB/s

Error Rate:
  record-error-rate        Current: 7.5 msg/s (0.05%)
  record-retry-rate        Current: 150 msg/s (1%)
  
Alert: error-rate > 1% for 5 minutes

Latency:
  record-queue-time-avg    Current: 12ms
  request-latency-avg      Current: 45ms
  request-latency-p99      Current: 120ms
  
Alert: p99-latency > 500ms for 10 minutes

Resources:
  buffer-available-bytes   Current: 18 MB (56% free)
  buffer-exhausted-rate    Current: 0 events/s
  
Alert: buffer-available < 20% for 5 minutes

Connections:
  connection-count         Current: 3 (3 brokers)
  connection-close-rate    Current: 0 events/s
  
Alert: connection-close-rate > 1/minute

Grafana Dashboard Query Examples:
───────────────────────────────────────────────────
Rate of errors (last 5m):
  rate(kafka_producer_record_error_total[5m])

P99 latency:
  histogram_quantile(0.99, 
    rate(kafka_producer_request_latency_bucket[5m]))

Error percentage:
  (rate(kafka_producer_record_error_total[5m]) 
   / rate(kafka_producer_record_send_total[5m])) * 100
EOF
}

cleanup() {
  log_info "Cleaning up test topic"
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TEST_TOPIC" \
    --if-exists 2>/dev/null || true
}

main() {
  log_section "Kafka Error Handling Test Suite"
  
  log_info "Configuration:"
  echo "  Topic:      $TEST_TOPIC"
  echo "  Bootstrap:  $KAFKA_BOOTSTRAP_SERVERS"
  
  # Setup
  create_test_topic
  
  # Run tests
  test_successful_send
  test_timeout_error
  test_invalid_topic
  test_large_message
  test_nonexistent_broker
  test_retry_behavior
  test_error_logging
  demonstrate_monitoring
  
  # Cleanup
  cleanup
  
  log_section "Test Suite Complete"
  
  echo ""
  echo "Key Takeaways:"
  echo "  1. Always handle errors - don't use fire-and-forget"
  echo "  2. Differentiate retriable vs non-retriable errors"
  echo "  3. Route non-retriable errors to DLQ"
  echo "  4. Log errors with structured data and context"
  echo "  5. Monitor error rates and set up alerts"
  echo "  6. Include correlation IDs for tracing"
  echo ""
  echo "Next Steps:"
  echo "  • Implement DLQ routing (see demo_dlq.sh)"
  echo "  • Set up metrics monitoring (see monitor_metrics.sh)"
  echo "  • Configure alerting rules"
  echo "  • Test error handling in your application"
  echo ""
}

trap cleanup EXIT

main "$@"
