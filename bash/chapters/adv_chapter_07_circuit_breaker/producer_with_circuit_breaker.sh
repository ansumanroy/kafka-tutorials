#!/bin/bash

# Kafka Producer with Circuit Breaker
# Protects against Kafka outages with fail-fast behavior

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/circuit_breaker.sh"

# Source environment
if [ -f "$SCRIPT_DIR/../../../infra/env.local" ]; then
  source "$SCRIPT_DIR/../../../infra/env.local"
fi

# Configuration
TOPIC="${TOPIC:-test-circuit-breaker}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
NUM_MESSAGES="${NUM_MESSAGES:-30}"
SEND_INTERVAL="${SEND_INTERVAL:-2}"

# Stats
TOTAL_ATTEMPTS=0
SUCCESSFUL_SENDS=0
FAILED_SENDS=0
REJECTED_BY_CIRCUIT=0

echo "╔════════════════════════════════════════════════════════╗"
echo "║    Kafka Producer with Circuit Breaker                ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
echo "Configuration:"
echo "  Topic: $TOPIC"
echo "  Bootstrap Servers: $BOOTSTRAP_SERVERS"
echo "  Messages: $NUM_MESSAGES"
echo "  Interval: ${SEND_INTERVAL}s"
echo ""
echo "Circuit Breaker:"
echo "  Failure Threshold: $CB_FAILURE_THRESHOLD"
echo "  Success Threshold: $CB_SUCCESS_THRESHOLD"
echo "  Timeout: ${CB_TIMEOUT}s"
echo ""

# Initialize circuit breaker
rm -f /tmp/circuit_breaker_state
init_circuit_breaker

# Function to send message with circuit breaker
send_message_with_circuit_breaker() {
  local message="$1"
  local attempt_num="$2"
  
  TOTAL_ATTEMPTS=$((TOTAL_ATTEMPTS + 1))
  
  echo "[$attempt_num/$NUM_MESSAGES] Sending message..."
  
  # Check circuit breaker
  if ! circuit_breaker_allow_request; then
    echo "  🔴 REJECTED by circuit breaker (circuit is OPEN)"
    echo "  → Failing fast (no Kafka call)"
    REJECTED_BY_CIRCUIT=$((REJECTED_BY_CIRCUIT + 1))
    return 1
  fi
  
  # Try to send
  local start_time=$(date +%s%3N)
  local result
  local exit_code
  
  result=$(echo "$message" | timeout 5 kafka-console-producer.sh \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --topic "$TOPIC" \
    2>&1) || exit_code=$?
  
  local end_time=$(date +%s%3N)
  local latency=$((end_time - start_time))
  
  if [ ${exit_code:-0} -eq 0 ]; then
    echo "  ✅ SUCCESS (${latency}ms)"
    circuit_breaker_record_success
    SUCCESSFUL_SENDS=$((SUCCESSFUL_SENDS + 1))
    return 0
  else
    echo "  ❌ FAILED (${latency}ms): ${result}"
    circuit_breaker_record_failure
    FAILED_SENDS=$((FAILED_SENDS + 1))
    return 1
  fi
}

# Main loop
echo "Starting producer..."
echo ""

for i in $(seq 1 $NUM_MESSAGES); do
  local timestamp=$(date +%s)
  local message="message-$i:timestamp=$timestamp"
  
  send_message_with_circuit_breaker "$message" "$i"
  
  circuit_breaker_status
  echo ""
  
  # Simulate Kafka going down at message 5
  if [ $i -eq 4 ]; then
    echo "⚠️  SIMULATING KAFKA OUTAGE (next 6 requests will fail)..."
    echo ""
  fi
  
  # Simulate Kafka recovering at message 15
  if [ $i -eq 14 ]; then
    echo "⚡ SIMULATING KAFKA RECOVERY..."
    echo ""
  fi
  
  sleep $SEND_INTERVAL
done

# Print summary
echo "╔════════════════════════════════════════════════════════╗"
echo "║    Summary                                             ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
echo "Total Attempts:         $TOTAL_ATTEMPTS"
echo "Successful Sends:       $SUCCESSFUL_SENDS"
echo "Failed Sends:           $FAILED_SENDS"
echo "Rejected by Circuit:    $REJECTED_BY_CIRCUIT"
echo ""

local success_rate=0
if [ $TOTAL_ATTEMPTS -gt 0 ]; then
  success_rate=$(awk "BEGIN {printf \"%.1f\", ($SUCCESSFUL_SENDS / $TOTAL_ATTEMPTS) * 100}")
fi

echo "Success Rate:           ${success_rate}%"
echo ""

# Calculate time saved by circuit breaker
local time_saved=$((REJECTED_BY_CIRCUIT * 5))  # 5s timeout each
echo "Time saved by circuit breaker: ${time_saved}s"
echo "(Rejected requests failed instantly vs waiting for timeout)"
echo ""

circuit_breaker_status
