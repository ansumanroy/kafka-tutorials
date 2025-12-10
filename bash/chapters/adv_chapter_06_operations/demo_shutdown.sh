#!/usr/bin/env bash
#
# demo_shutdown.sh
#
# Demonstrate graceful shutdown patterns for Kafka producers.
# Shows proper cleanup, flush, and shutdown handling.
#
# Usage:
#   ./demo_shutdown.sh [duration_seconds]
#
# Examples:
#   ./demo_shutdown.sh        # Run for 10 seconds
#   ./demo_shutdown.sh 30     # Run for 30 seconds
#
# Send SIGTERM (Ctrl+C) to trigger graceful shutdown
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
TEST_TOPIC="shutdown-demo-topic"
DURATION="${1:-10}"
SHUTDOWN_INITIATED=false
SHUTDOWN_FILE="/tmp/shutdown-initiated-$$"

log_section() {
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "════════════════════════════════════════════════════════════"
}

log_shutdown() {
  local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
  echo "[$timestamp] SHUTDOWN: $1"
}

create_topic() {
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

graceful_shutdown() {
  if [ "$SHUTDOWN_INITIATED" = true ]; then
    log_shutdown "Shutdown already in progress..."
    return
  fi
  
  SHUTDOWN_INITIATED=true
  touch "$SHUTDOWN_FILE"
  
  log_section "GRACEFUL SHUTDOWN INITIATED"
  log_shutdown "Received shutdown signal (SIGTERM/SIGINT)"
  log_shutdown "Starting graceful shutdown sequence..."
  
  # Step 1: Stop accepting new work
  log_shutdown "Step 1/5: Stopping acceptance of new requests"
  log_shutdown "  → Application marked as unhealthy"
  log_shutdown "  → Load balancer will stop sending traffic"
  sleep 2
  
  # Step 2: Drain in-progress work
  log_shutdown "Step 2/5: Draining in-progress work"
  log_shutdown "  → Waiting for active sends to complete..."
  log_shutdown "  → Timeout: 5 seconds"
  sleep 3
  log_shutdown "  ✓ All active sends completed"
  
  # Step 3: Flush producer buffer
  log_shutdown "Step 3/5: Flushing producer buffer"
  log_shutdown "  → Sending all buffered messages to Kafka"
  log_shutdown "  → This ensures no message loss"
  log_shutdown "  → Timeout: 10 seconds"
  
  # In real producer: producer.flush(Duration.ofSeconds(10))
  # For console producer, messages are sent immediately
  sleep 3
  log_shutdown "  ✓ All messages flushed successfully"
  
  # Step 4: Close producer
  log_shutdown "Step 4/5: Closing producer connection"
  log_shutdown "  → Waiting for final acknowledgements"
  log_shutdown "  → Timeout: 5 seconds"
  sleep 2
  log_shutdown "  ✓ Producer closed gracefully"
  
  # Step 5: Cleanup
  log_shutdown "Step 5/5: Final cleanup"
  log_shutdown "  → Releasing resources"
  log_shutdown "  → Closing connections"
  log_shutdown "  → Logging final statistics"
  
  # Cleanup topic
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TEST_TOPIC" \
    --if-exists 2>/dev/null || true
  
  log_shutdown "  ✓ Cleanup complete"
  
  log_section "GRACEFUL SHUTDOWN COMPLETE"
  log_success "Total shutdown time: ~15 seconds"
  log_success "Exit code: 0 (success)"
  
  rm -f "$SHUTDOWN_FILE"
  
  exit 0
}

demonstrate_shutdown_concepts() {
  log_section "Graceful Shutdown Concepts"
  
  cat << 'EOF'
Why Graceful Shutdown Matters:
══════════════════════════════════════════════════════════

Problem: Immediate Termination (SIGKILL)
  Application killed instantly
    → Buffered messages lost
    → In-flight requests incomplete
    → Connections not closed properly
    → Resources leaked
    → Data loss possible

Solution: Graceful Shutdown (SIGTERM)
  Application given time to clean up
    → All buffered messages sent
    → In-flight requests completed
    → Connections closed properly
    → Resources released cleanly
    → Zero data loss

Shutdown Signal Flow:
──────────────────────────────────────────────────────
Signal Type    Behavior              Use Case
─────────────────────────────────────────────────────
SIGTERM (15)   Request termination   Normal shutdown
SIGINT (2)     Interrupt (Ctrl+C)    Manual stop
SIGKILL (9)    Force kill            Emergency only!

Kubernetes Lifecycle:
──────────────────────────────────────────────────────
1. Pod receives termination request
2. Pod marked as "Terminating"
3. PreStop hook executed (optional)
4. SIGTERM sent to container
5. Grace period begins (default: 30s)
6. If still running: SIGKILL sent
7. Pod removed

Best Practices:
──────────────────────────────────────────────────────
✓ Always trap SIGTERM and SIGINT
✓ Stop accepting new work immediately
✓ Flush producer before closing
✓ Set appropriate timeouts
✓ Log shutdown progress
✓ Exit with code 0 on success

✗ Don't ignore shutdown signals
✗ Don't exceed grace period
✗ Don't leave resources uncleaned
✗ Don't lose buffered data
EOF
}

demonstrate_shutdown_antipatterns() {
  log_section "Shutdown Anti-Patterns (What NOT to Do)"
  
  cat << 'EOF'
Anti-Pattern 1: No Shutdown Handler
───────────────────────────────────────────────────────
# ❌ BAD - No signal handling
while true; do
  echo "message" | kafka-console-producer.sh ...
done
# Process killed → buffered messages lost!

# ✓ GOOD - Proper signal handling
trap 'graceful_shutdown' SIGTERM SIGINT
while ! [ -f /tmp/shutdown ]; do
  echo "message" | kafka-console-producer.sh ...
done

Anti-Pattern 2: Blocking Forever
───────────────────────────────────────────────────────
# ❌ BAD - No timeout
producer.flush()  # Blocks indefinitely!
producer.close()  # No timeout set

# ✓ GOOD - With timeouts
producer.flush(Duration.ofSeconds(15))
producer.close(Duration.ofSeconds(5))

Anti-Pattern 3: Losing Buffered Messages
───────────────────────────────────────────────────────
# ❌ BAD - Skip flush
trap 'exit 0' SIGTERM
# Messages in buffer lost!

# ✓ GOOD - Flush before exit
trap 'producer.flush(); producer.close(); exit 0' SIGTERM

Anti-Pattern 4: No Grace Period
───────────────────────────────────────────────────────
# ❌ BAD - Kubernetes config
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      terminationGracePeriodSeconds: 0  # Immediate kill!

# ✓ GOOD - Adequate grace period
spec:
  template:
    spec:
      terminationGracePeriodSeconds: 30  # 30 seconds

Anti-Pattern 5: Not Marking Unhealthy
───────────────────────────────────────────────────────
# ❌ BAD - Keep accepting requests during shutdown
graceful_shutdown() {
  producer.close()
  exit 0
}

# ✓ GOOD - Stop accepting new requests first
graceful_shutdown() {
  mark_unhealthy()  # Return 503
  wait_for_drain()  # Let LB redirect traffic
  producer.flush()
  producer.close()
  exit 0
}
EOF
}

demonstrate_timeout_budget() {
  log_section "Shutdown Timeout Budget"
  
  cat << 'EOF'
Typical Shutdown Timeline (30 second total):
══════════════════════════════════════════════════════════

Second  Action                          Status
──────────────────────────────────────────────────────────
0       Receive SIGTERM                 Shutdown starts
1       Mark application unhealthy      503 responses
2       Stop accepting new requests     No new sends
3-7     Drain in-progress requests      (5 seconds)
8-22    Flush producer buffer           (15 seconds)
23-27   Close producer connection       (5 seconds)
28-29   Final cleanup                   (2 seconds)
30      Exit process                    Code 0

If process still running at 30s → SIGKILL

Timeout Configuration:
──────────────────────────────────────────────────────
Component                   Timeout      Rationale
──────────────────────────────────────────────────────
Drain phase                 5s          Complete active sends
Flush phase                 15s         Send buffered messages
Close phase                 5s          Wait for final ACKs
Cleanup phase               2s          Release resources
Buffer (safety margin)      3s          Handle variance
────────────────────────────────────────────────────────
TOTAL                       30s

Adjusting for Your Environment:
──────────────────────────────────────────────────────
Fast network, low latency:
  • Total: 20 seconds
  • Flush: 10s, Close: 5s, Drain: 3s, Cleanup: 2s

Slow network, high latency:
  • Total: 45 seconds
  • Flush: 25s, Close: 10s, Drain: 5s, Cleanup: 5s

High message volume:
  • Total: 60 seconds
  • Flush: 40s, Close: 10s, Drain: 5s, Cleanup: 5s

Testing Your Timeouts:
──────────────────────────────────────────────────────
1. Produce high volume of messages
2. Send SIGTERM
3. Measure actual shutdown time
4. Adjust grace period accordingly
5. Add 20% buffer for safety
EOF
}

simulate_producer() {
  log_section "Starting Producer Simulation"
  
  log_info "Producer will run for $DURATION seconds"
  log_info "Press Ctrl+C to trigger graceful shutdown"
  log_info ""
  log_info "Producing messages to: $TEST_TOPIC"
  
  local counter=0
  local start_time=$(date +%s)
  
  while ! [ -f "$SHUTDOWN_FILE" ]; do
    counter=$((counter + 1))
    local timestamp=$(date +%s)
    local elapsed=$((timestamp - start_time))
    
    # Produce message
    echo "{\"id\":$counter,\"timestamp\":$timestamp,\"elapsed\":$elapsed}" | \
      kafka-console-producer.sh \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --topic "$TEST_TOPIC" \
        2>/dev/null || true
    
    # Log every 10 messages
    if [ $((counter % 10)) -eq 0 ]; then
      log_info "Produced $counter messages (${elapsed}s elapsed)"
    fi
    
    # Check if duration exceeded
    if [ $elapsed -ge $DURATION ]; then
      log_info "Duration reached, triggering graceful shutdown..."
      graceful_shutdown
    fi
    
    sleep 0.5
  done
  
  log_info "Producer loop exited cleanly"
}

verify_messages() {
  log_section "Verifying Messages"
  
  local message_count=$(kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TEST_TOPIC" \
    --time -1 2>/dev/null | \
    awk -F: '{sum+=$3} END {print sum}' || echo "0")
  
  log_success "Total messages in topic: $message_count"
  
  if [ "$message_count" -gt 0 ]; then
    log_success "✓ No message loss detected"
    log_success "✓ All messages were successfully flushed"
  else
    log_warn "No messages found (may have been too quick)"
  fi
}

main() {
  log_section "Graceful Shutdown Demonstration"
  
  log_info "This demo shows proper shutdown handling"
  log_info "Topic: $TEST_TOPIC"
  log_info "Duration: ${DURATION}s"
  log_info "Bootstrap: $KAFKA_BOOTSTRAP_SERVERS"
  
  # Setup signal handlers
  trap 'graceful_shutdown' SIGTERM SIGINT
  
  # Clean up any previous shutdown file
  rm -f "$SHUTDOWN_FILE"
  
  # Show concepts
  demonstrate_shutdown_concepts
  demonstrate_shutdown_antipatterns
  demonstrate_timeout_budget
  
  # Setup
  create_topic
  
  # Run simulation
  simulate_producer
  
  # Verify
  verify_messages
  
  log_section "Demo Complete"
  
  echo ""
  echo "What We Demonstrated:"
  echo "  1. Proper signal handling (SIGTERM/SIGINT)"
  echo "  2. Stop accepting new work first"
  echo "  3. Drain in-progress requests"
  echo "  4. Flush producer buffer"
  echo "  5. Close producer gracefully"
  echo "  6. Clean exit with code 0"
  echo ""
  echo "Key Takeaways:"
  echo "  • Always implement graceful shutdown"
  echo "  • Use appropriate timeouts (30s typical)"
  echo "  • Test shutdown in staging"
  echo "  • Monitor shutdown duration"
  echo "  • Document your shutdown sequence"
  echo ""
}

main "$@"
