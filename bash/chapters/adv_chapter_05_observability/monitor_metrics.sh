#!/usr/bin/env bash
#
# monitor_metrics.sh
#
# Monitor Kafka producer metrics and demonstrate observability.
# Shows key metrics, health checks, and alerting concepts.
#
# Usage:
#   ./monitor_metrics.sh [topic_name] [duration_seconds]
#
# Examples:
#   ./monitor_metrics.sh                    # Use defaults
#   ./monitor_metrics.sh my-topic 30       # Monitor for 30 seconds
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
TOPIC_NAME="${1:-metrics-test}"
DURATION="${2:-10}"

log_section() {
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "════════════════════════════════════════════════════════════"
}

log_metric() {
  echo ""
  echo "────────────────────────────────────────────────────────────"
  echo "  $1"
  echo "────────────────────────────────────────────────────────────"
}

create_topic() {
  log_info "Creating test topic: $TOPIC_NAME"
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  
  sleep 2
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions 3 \
    --replication-factor 1
  
  sleep 2
  log_success "Topic created"
}

demonstrate_key_metrics() {
  log_metric "Producer Metrics Overview"
  
  cat << 'EOF'
Kafka Producer Metrics Categories:
═══════════════════════════════════════════════════════

1. THROUGHPUT METRICS
───────────────────────────────────────────────────────
Metric: record-send-rate
  Description: Records sent per second
  Type: Rate
  Alert: < expected_rate (throughput drop)
  Typical: 1K-100K records/sec depending on use case

Metric: byte-rate
  Description: Bytes sent per second
  Type: Rate  
  Alert: Unusual spikes/drops
  Typical: Varies by message size

Metric: records-per-request-avg
  Description: Average records per batch
  Type: Gauge
  Alert: < 10 (poor batching efficiency)
  Typical: 50-500 for good batching

2. LATENCY METRICS
───────────────────────────────────────────────────────
Metric: record-queue-time-avg
  Description: Time message waits in buffer
  Type: Gauge (milliseconds)
  Alert: > 100ms (buffer pressure)
  Typical: 1-20ms

Metric: request-latency-avg
  Description: Network + broker processing time
  Type: Gauge (milliseconds)
  Alert: > 100ms (network/broker slow)
  Typical: 10-50ms

Metric: request-latency-max
  Description: Maximum request latency
  Type: Gauge (milliseconds)
  Alert: > 1000ms (serious issue)
  Typical: < 200ms

3. ERROR METRICS
───────────────────────────────────────────────────────
Metric: record-error-rate
  Description: Failed records per second
  Type: Rate
  Alert: > 0.1% of send-rate
  Typical: 0 or very low

Metric: record-retry-rate
  Description: Retried records per second
  Type: Rate
  Alert: > 5% of send-rate
  Typical: < 1%

Metric: record-send-total
  Description: Total records sent (cumulative)
  Type: Counter
  Use: Calculate rates and percentages

4. RESOURCE METRICS
───────────────────────────────────────────────────────
Metric: buffer-available-bytes
  Description: Free space in producer buffer
  Type: Gauge (bytes)
  Alert: < 20% of buffer.memory
  Typical: > 50% free

Metric: buffer-total-bytes
  Description: Total buffer size (buffer.memory)
  Type: Gauge (bytes)
  Typical: 32 MB (33554432 bytes)

Metric: waiting-threads
  Description: Threads blocked on send()
  Type: Gauge
  Alert: > 0 consistently
  Typical: 0

5. CONNECTION METRICS
───────────────────────────────────────────────────────
Metric: connection-count
  Description: Active broker connections
  Type: Gauge
  Typical: Number of brokers (usually 3-5)

Metric: connection-close-rate
  Description: Rate of connection closures
  Type: Rate
  Alert: > 1 per minute
  Typical: 0

Metric: failed-authentication-rate
  Description: Authentication failures
  Type: Rate
  Alert: > 0
  Typical: 0
EOF
}

simulate_producer_activity() {
  log_metric "Starting Producer Activity"
  
  echo "Producing messages to $TOPIC_NAME for ${DURATION} seconds..."
  echo "  Rate: ~10 messages/second"
  
  (
    for i in $(seq 1 $((DURATION * 10))); do
      echo "{\"id\":$i,\"timestamp\":$(date +%s),\"data\":\"test message $i\"}"
      sleep 0.1
    done
  ) | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    2>/dev/null &
  
  local producer_pid=$!
  
  log_success "Producer started (PID: $producer_pid)"
  echo "  Duration: ${DURATION}s"
  
  # Wait for producer to finish
  wait $producer_pid 2>/dev/null || true
  
  log_success "Producer activity completed"
}

show_topic_metrics() {
  log_metric "Topic-Level Metrics"
  
  echo "Fetching topic information..."
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --describe --topic "$TOPIC_NAME" \
    2>/dev/null || true
  
  echo ""
  echo "Message count per partition:"
  kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --time -1 2>/dev/null | \
    awk -F: '{printf "  Partition %s: %d messages\n", $2, $3}' || true
  
  echo ""
  echo "Total messages:"
  kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --time -1 2>/dev/null | \
    awk -F: '{sum+=$3} END {printf "  %d messages\n", sum}' || echo "  Unable to calculate"
}

demonstrate_health_checks() {
  log_metric "Health Check Patterns"
  
  cat << 'EOF'
Producer Health Checks:
═══════════════════════════════════════════════════════

1. Connectivity Check
───────────────────────────────────────────────────────
Purpose: Verify producer can connect to brokers

Check:
  • Producer initialized successfully
  • Active connections to brokers
  • Metadata fetch succeeds

Implementation:
  try {
    producer.partitionsFor(topic);
    return healthy;
  } catch (Exception e) {
    return unhealthy;
  }

2. Send Health Check
───────────────────────────────────────────────────────
Purpose: Verify producer can actually send messages

Check:
  • Send test message to health-check topic
  • Verify successful acknowledgement
  • Check latency is acceptable

Implementation:
  start = now()
  producer.send(healthCheckTopic, testMessage).get()
  latency = now() - start
  return (latency < threshold) ? healthy : degraded

3. Metrics-Based Health
───────────────────────────────────────────────────────
Check metrics to determine health status:

Healthy:
  ✓ error-rate = 0
  ✓ connection-count = expected
  ✓ buffer-available > 20%
  ✓ request-latency-p99 < 200ms

Degraded:
  ⚠ error-rate > 0 but < 1%
  ⚠ retry-rate elevated
  ⚠ latency slightly high
  ⚠ buffer pressure moderate

Unhealthy:
  ✗ error-rate > 1%
  ✗ connections = 0
  ✗ buffer exhausted
  ✗ latency > 1000ms

4. Readiness vs Liveness
───────────────────────────────────────────────────────
Readiness (Can accept traffic?):
  • Producer initialized
  • Brokers reachable
  • Test send succeeds

Liveness (Should restart?):
  • Producer not shutdown
  • No fatal errors
  • Connections stable

Kubernetes Example:
  livenessProbe:
    httpGet:
      path: /health/live
      port: 8080
    periodSeconds: 30
  
  readinessProbe:
    httpGet:
      path: /health/ready
      port: 8080
    periodSeconds: 10
EOF
}

demonstrate_alerting_rules() {
  log_metric "Alerting Rules & Thresholds"
  
  cat << 'EOF'
Alert Configuration Examples:
═══════════════════════════════════════════════════════

CRITICAL ALERTS (Page Immediately)
───────────────────────────────────────────────────────
1. Producer Error Rate High
   Condition: error_rate > 1% of send_rate
   Duration: 2 minutes
   Action: Page on-call engineer
   
   Prometheus:
     (rate(kafka_producer_record_error_total[5m]) 
      / rate(kafka_producer_record_send_total[5m])) > 0.01

2. Producer Down
   Condition: No metrics received
   Duration: 1 minute
   Action: Page on-call engineer
   
   Prometheus:
     up{job="kafka-producer"} == 0

3. All Connections Lost
   Condition: connection_count = 0
   Duration: 30 seconds
   Action: Page on-call engineer
   
   Prometheus:
     kafka_producer_connection_count == 0

WARNING ALERTS (Investigate Soon)
───────────────────────────────────────────────────────
1. High Latency
   Condition: p99_latency > 500ms
   Duration: 10 minutes
   Action: Slack notification
   
   Prometheus:
     histogram_quantile(0.99,
       rate(kafka_producer_request_latency_bucket[5m])) > 0.5

2. Buffer Pressure
   Condition: buffer_available < 20%
   Duration: 5 minutes
   Action: Slack notification
   
   Prometheus:
     (kafka_producer_buffer_available_bytes 
      / kafka_producer_buffer_total_bytes) < 0.2

3. High Retry Rate
   Condition: retry_rate > 5% of send_rate
   Duration: 10 minutes
   Action: Slack notification
   
   Prometheus:
     (rate(kafka_producer_record_retry_total[5m])
      / rate(kafka_producer_record_send_total[5m])) > 0.05

INFO ALERTS (Track Trends)
───────────────────────────────────────────────────────
1. Throughput Drop
   Condition: send_rate < 80% of baseline
   Duration: 30 minutes
   Action: Log/ticket
   
2. Batching Inefficiency
   Condition: records_per_request < 10
   Duration: 1 hour
   Action: Log/review

3. Connection Churn
   Condition: connection_close_rate > 0
   Duration: 5 minutes
   Action: Log

Alert Best Practices:
───────────────────────────────────────────────────────
✓ Set duration thresholds (avoid flapping)
✓ Use percentile metrics (p95, p99) not averages
✓ Include runbooks in alert descriptions
✓ Test alerts with simulations
✓ Review and tune thresholds regularly
✓ Group related alerts (alert fatigue)

✗ Don't alert on every fluctuation
✗ Don't ignore alert history
✗ Don't have orphaned alerts (nobody responds)
EOF
}

demonstrate_grafana_dashboard() {
  log_metric "Grafana Dashboard Design"
  
  cat << 'EOF'
Kafka Producer Dashboard Layout:
═══════════════════════════════════════════════════════

ROW 1: OVERVIEW (Single Stat Panels)
───────────────────────────────────────────────────────
┌──────────────────────────────────────────────────────┐
│ Throughput      Error Rate      P99 Latency   Uptime │
│ 15,234 msg/s    0.02%          45ms          99.9%   │
└──────────────────────────────────────────────────────┘

ROW 2: THROUGHPUT (Time Series)
───────────────────────────────────────────────────────
┌──────────────────────────────────────────────────────┐
│ Records/sec                                          │
│ ▁▂▃▅▇█▇▅▃▂▁▂▃▅▇█▇▅▃▂▁                              │
└──────────────────────────────────────────────────────┘

ROW 3: LATENCY (Time Series)
───────────────────────────────────────────────────────
┌──────────────────────────────────────────────────────┐
│ Request Latency (p50, p95, p99)                     │
│ p99: ▁▂▃▅▇█▇▅▃▂▁                                    │
│ p95: ▁▂▃▄▅▆▅▄▃▂▁                                    │
│ p50: ▁▁▂▂▃▃▂▂▁▁                                    │
└──────────────────────────────────────────────────────┘

ROW 4: ERRORS (Time Series + Table)
───────────────────────────────────────────────────────
┌──────────────┬───────────────────────────────────────┐
│ Error Rate   │ Recent Errors                         │
│ ▁▁▁▁▂▃▂▁▁▁  │ Type          Count  Last Seen        │
│              │ TIMEOUT       12     2m ago           │
│              │ TOO_LARGE     3      15m ago          │
└──────────────┴───────────────────────────────────────┘

ROW 5: RESOURCES (Gauges)
───────────────────────────────────────────────────────
┌──────────────────────────────────────────────────────┐
│ Buffer Usage           Connections                   │
│ ████████░░░░ 68%      Active: 3/3                   │
└──────────────────────────────────────────────────────┘

Query Examples:
───────────────────────────────────────────────────────
Throughput:
  rate(kafka_producer_record_send_total[1m])

Error %:
  (rate(kafka_producer_record_error_total[5m])
   / rate(kafka_producer_record_send_total[5m])) * 100

P99 Latency:
  histogram_quantile(0.99,
    sum(rate(kafka_producer_request_latency_bucket[5m]))
    by (le))

Buffer Usage %:
  (1 - (kafka_producer_buffer_available_bytes
   / kafka_producer_buffer_total_bytes)) * 100
EOF
}

cleanup() {
  log_info "Cleaning up test topic"
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
}

main() {
  log_section "Kafka Producer Metrics & Monitoring"
  
  log_info "Configuration:"
  echo "  Topic:      $TOPIC_NAME"
  echo "  Duration:   ${DURATION}s"
  echo "  Bootstrap:  $KAFKA_BOOTSTRAP_SERVERS"
  
  # Setup
  create_topic
  
  # Demonstrate concepts
  demonstrate_key_metrics
  
  # Simulate activity and show metrics
  simulate_producer_activity
  show_topic_metrics
  
  # Show observability patterns
  demonstrate_health_checks
  demonstrate_alerting_rules
  demonstrate_grafana_dashboard
  
  # Cleanup
  cleanup
  
  log_section "Monitoring Demonstration Complete"
  
  echo ""
  echo "Key Takeaways:"
  echo "  1. Monitor key metrics: throughput, latency, errors"
  echo "  2. Set up dashboards for visualization"
  echo "  3. Configure alerts with appropriate thresholds"
  echo "  4. Implement health checks (liveness/readiness)"
  echo "  5. Use structured logging for correlation"
  echo ""
  echo "Implementation Steps:"
  echo "  □ Export metrics (JMX, Prometheus, etc.)"
  echo "  □ Create Grafana dashboard"
  echo "  □ Set up alerting rules"
  echo "  □ Implement health check endpoints"
  echo "  □ Add distributed tracing"
  echo "  □ Test alerts with failure scenarios"
  echo ""
}

trap cleanup EXIT

main "$@"
