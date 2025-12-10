#!/usr/bin/env bash
#
# demo_dlq.sh
#
# Demonstrate Dead Letter Queue (DLQ) pattern for handling failed messages.
# Shows how to route failed messages to a DLQ topic with error metadata.
#
# Usage:
#   ./demo_dlq.sh [main_topic] [dlq_topic]
#
# Examples:
#   ./demo_dlq.sh                               # Use defaults
#   ./demo_dlq.sh user-events user-events-dlq  # Custom topics
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
MAIN_TOPIC="${1:-main-topic}"
DLQ_TOPIC="${2:-${MAIN_TOPIC}-dlq}"

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

create_topics() {
  log_info "Creating main and DLQ topics..."
  
  # Main topic
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$MAIN_TOPIC" \
    --if-exists 2>/dev/null || true
  
  sleep 1
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$MAIN_TOPIC" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000  # 1 day
  
  # DLQ topic with longer retention
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$DLQ_TOPIC" \
    --if-exists 2>/dev/null || true
  
  sleep 1
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$DLQ_TOPIC" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000  # 7 days (longer for investigation)
  
  sleep 2
  log_success "Topics created"
  echo "  Main topic: $MAIN_TOPIC (retention: 1 day)"
  echo "  DLQ topic:  $DLQ_TOPIC (retention: 7 days)"
}

demonstrate_dlq_concept() {
  log_demo "Demo 1: DLQ Concept Overview"
  
  cat << 'EOF'
Dead Letter Queue (DLQ) Pattern:
═══════════════════════════════════════════════════════

Purpose:
  Store messages that failed processing for later investigation
  and potential reprocessing.

When to Use DLQ:
  ✓ Non-retriable errors (RECORD_TOO_LARGE, invalid data)
  ✓ Deserialization failures
  ✓ Business validation failures
  ✓ Exhausted retry attempts
  ✗ Retriable errors (let producer retry)
  ✗ Expected failures (handle normally)

DLQ Architecture:
──────────────────────────────────────────────────────

Normal Flow:
  Producer → [Main Topic] → Consumer → Success ✓

Error Flow:
  Producer → [Main Topic] → Consumer → Validation Error ✗
                                          ↓
                            DLQ Producer → [DLQ Topic]
                                          ↓
                            Store with metadata:
                              • Original message
                              • Error details
                              • Timestamp
                              • Retry count
                              • Source info

Investigation Flow:
  [DLQ Topic] → Monitor/Alert → Human Investigation
                              ↓
                        Fix Issue (code/data/config)
                              ↓
                        Replay from DLQ
                              ↓
                   [Main Topic] → Consumer → Success ✓
EOF
}

demonstrate_dlq_message_format() {
  log_demo "Demo 2: DLQ Message Format"
  
  echo "Example DLQ message with full context:"
  echo ""
  
  cat << 'EOF' | jq '.' 2>/dev/null || cat
{
  "dlqMetadata": {
    "originalTopic": "user-events",
    "originalPartition": 2,
    "originalOffset": 12345,
    "originalTimestamp": 1640000000000,
    "dlqTimestamp": 1640000120000,
    "retryCount": 3,
    "source": "order-service-v2.1.0",
    "hostname": "app-server-01.prod"
  },
  "error": {
    "type": "RECORD_TOO_LARGE",
    "message": "Message size 2097152 exceeds maximum 1048576",
    "code": "MSG_TOO_LARGE_001",
    "retriable": false,
    "stackTrace": "com.example.ProducerService.send(...)"
  },
  "originalMessage": {
    "key": "order-12345",
    "value": "{\"orderId\":\"12345\",\"items\":[...]}}",
    "headers": {
      "correlation-id": "abc-123-def-456",
      "trace-id": "550e8400-e29b-41d4-a716-446655440000",
      "content-type": "application/json",
      "schema-version": "2.0"
    }
  },
  "diagnostics": {
    "messageSizeBytes": 2097152,
    "maxAllowedBytes": 1048576,
    "compressionType": "none",
    "suggestion": "Split order items into multiple messages"
  }
}
EOF
  
  echo ""
  echo "Key elements:"
  echo "  • dlqMetadata: Context about original message"
  echo "  • error: Detailed error information"
  echo "  • originalMessage: Full original message (key, value, headers)"
  echo "  • diagnostics: Helpful debugging information"
}

simulate_successful_send() {
  log_demo "Demo 3: Normal Message (No DLQ)"
  
  echo "Sending valid message to main topic..."
  
  local message='{"userId":"user-123","eventType":"login","timestamp":1640000000}'
  
  echo "$message" | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$MAIN_TOPIC" \
    2>/dev/null
  
  sleep 2
  
  log_success "Message sent successfully"
  
  echo ""
  echo "Verification:"
  timeout 2 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$MAIN_TOPIC" \
    --from-beginning \
    --max-messages 1 \
    2>/dev/null || true
  
  echo ""
  echo "Result: Message in main topic, not in DLQ ✓"
}

simulate_failed_message() {
  log_demo "Demo 4: Failed Message → DLQ Routing"
  
  echo "Simulating failed message that should go to DLQ..."
  echo ""
  
  # Create a DLQ message with error metadata
  local original_message='{"userId":"user-999","eventType":"purchase","amount":99.99}'
  local error_type="VALIDATION_ERROR"
  local error_message="Amount exceeds user credit limit"
  local timestamp=$(date +%s)000
  
  local dlq_message=$(cat << EOF
{
  "dlqMetadata": {
    "originalTopic": "$MAIN_TOPIC",
    "originalPartition": 0,
    "originalTimestamp": $timestamp,
    "dlqTimestamp": $timestamp,
    "retryCount": 3,
    "source": "demo-script"
  },
  "error": {
    "type": "$error_type",
    "message": "$error_message",
    "retriable": false
  },
  "originalMessage": {
    "key": "user-999",
    "value": $original_message
  }
}
EOF
)
  
  echo "Routing to DLQ: $DLQ_TOPIC"
  echo "$dlq_message" | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$DLQ_TOPIC" \
    2>/dev/null
  
  sleep 2
  
  log_success "Message routed to DLQ"
  
  echo ""
  echo "DLQ message content:"
  timeout 2 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$DLQ_TOPIC" \
    --from-beginning \
    --max-messages 1 \
    2>/dev/null | jq '.' 2>/dev/null || \
  timeout 2 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$DLQ_TOPIC" \
    --from-beginning \
    --max-messages 1 \
    2>/dev/null
  
  echo ""
  echo "Result: Failed message in DLQ with error metadata ✓"
}

monitor_dlq() {
  log_demo "Demo 5: Monitoring DLQ"
  
  echo "Checking DLQ message count..."
  
  local dlq_count=$(kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$DLQ_TOPIC" \
    --time -1 2>/dev/null | \
    awk -F: '{sum+=$3} END {print sum}' || echo "0")
  
  echo "  DLQ message count: $dlq_count"
  
  if [ "$dlq_count" -gt 0 ]; then
    log_warn "⚠️  DLQ contains $dlq_count message(s) - investigation needed"
  else
    log_success "DLQ is empty"
  fi
  
  echo ""
  echo "Monitoring strategies:"
  cat << 'EOF'

1. Message Count Alert:
   if (dlq_message_count > threshold) {
     alert("DLQ has messages - investigate failures")
   }

2. Growth Rate Alert:
   if (increase(dlq_messages[10m]) > 100) {
     alert("DLQ growing rapidly - systemic issue")
   }

3. Age Alert:
   if (oldest_dlq_message_age > 24h) {
     alert("Old DLQ messages not processed")
   }

4. Dashboard Metrics:
   • Total DLQ message count
   • DLQ message rate (msg/min)
   • Error type distribution
   • Top failing keys/sources
   • Average message age in DLQ
EOF
}

demonstrate_dlq_reprocessing() {
  log_demo "Demo 6: DLQ Reprocessing Strategy"
  
  cat << 'EOF'
Reprocessing Failed Messages:
═══════════════════════════════════════════════════════

Step 1: Investigate Root Cause
───────────────────────────────────────────────────────
• Review DLQ messages and error details
• Identify common failure patterns
• Determine if issue is:
    - Code bug (fix and deploy)
    - Bad data (clean/transform)
    - Configuration (update settings)
    - Capacity (scale resources)

Step 2: Fix the Issue
───────────────────────────────────────────────────────
• Deploy code fix
• Update configuration
• Scale infrastructure
• Clean/transform data

Step 3: Test the Fix
───────────────────────────────────────────────────────
• Test with sample DLQ messages
• Verify they now process successfully
• Confirm no new errors

Step 4: Replay from DLQ
───────────────────────────────────────────────────────
Option A: Manual replay
  # Read from DLQ and send to main topic
  kafka-console-consumer.sh \
    --topic user-events-dlq \
    --from-beginning | \
  kafka-console-producer.sh \
    --topic user-events

Option B: Automated replay service
  • Custom service monitors DLQ
  • Applies transformations/fixes
  • Retries with backoff
  • Moves back to main topic

Option C: Batch reprocessing
  • Extract DLQ messages
  • Process offline
  • Bulk reingest to main topic

Step 5: Verify and Clean Up
───────────────────────────────────────────────────────
• Confirm messages processed successfully
• Delete/archive DLQ messages
• Update runbooks with resolution
• Add monitoring to prevent recurrence

Best Practices:
───────────────────────────────────────────────────────
✓ Investigate before replaying (don't repeat errors)
✓ Test fix with small batch first
✓ Monitor during replay for new errors
✓ Document failure patterns and solutions
✓ Set up alerts to catch similar issues early

✗ Don't automatically replay without investigation
✗ Don't replay if root cause not fixed
✗ Don't delete DLQ messages before verification
EOF
}

demonstrate_dlq_naming() {
  log_demo "Demo 7: DLQ Naming Conventions"
  
  cat << 'EOF'
DLQ Topic Naming Patterns:
═══════════════════════════════════════════════════════

Pattern 1: Simple Suffix (Recommended for most cases)
───────────────────────────────────────────────────────
Topic: user-events
DLQ:   user-events-dlq

Pros: Simple, clear relationship
Cons: All errors mixed together

Pattern 2: Error-Type-Specific DLQs
───────────────────────────────────────────────────────
Topic: user-events
DLQs:  user-events-dlq-serialization
       user-events-dlq-validation
       user-events-dlq-timeout
       user-events-dlq-other

Pros: Easy to identify error patterns
Cons: More topics to manage

Pattern 3: Environment-Specific
───────────────────────────────────────────────────────
Topic: user-events
DLQs:  user-events-dlq-prod
       user-events-dlq-staging
       user-events-dlq-dev

Pros: Clear environment separation
Cons: Duplicate DLQ infrastructure

Pattern 4: Service-Specific
───────────────────────────────────────────────────────
Topic: user-events
DLQs:  user-events-dlq-api-gateway
       user-events-dlq-order-service
       user-events-dlq-notification-service

Pros: Clear ownership
Cons: Many DLQ topics

Recommendation:
  Start with Pattern 1 (simple suffix)
  Add specificity (Pattern 2) if needed for high-volume systems
EOF
}

cleanup() {
  log_info "Cleaning up demo topics"
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$MAIN_TOPIC" \
    --if-exists 2>/dev/null || true
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$DLQ_TOPIC" \
    --if-exists 2>/dev/null || true
}

main() {
  log_section "Dead Letter Queue (DLQ) Demonstration"
  
  log_info "Configuration:"
  echo "  Main topic: $MAIN_TOPIC"
  echo "  DLQ topic:  $DLQ_TOPIC"
  echo "  Bootstrap:  $KAFKA_BOOTSTRAP_SERVERS"
  
  # Setup
  create_topics
  
  # Run demos
  demonstrate_dlq_concept
  demonstrate_dlq_message_format
  simulate_successful_send
  simulate_failed_message
  monitor_dlq
  demonstrate_dlq_reprocessing
  demonstrate_dlq_naming
  
  # Cleanup
  cleanup
  
  log_section "Demonstration Complete"
  
  echo ""
  echo "Key Takeaways:"
  echo "  1. Use DLQ for non-retriable errors and exhausted retries"
  echo "  2. Include full context in DLQ messages (original + error)"
  echo "  3. Monitor DLQ size and growth rate"
  echo "  4. Set longer retention for DLQ (7+ days)"
  echo "  5. Investigate before replaying from DLQ"
  echo "  6. Document common failure patterns"
  echo ""
  echo "Implementation Checklist:"
  echo "  □ Create DLQ topic with appropriate retention"
  echo "  □ Route non-retriable errors to DLQ"
  echo "  □ Include error metadata in DLQ messages"
  echo "  □ Set up DLQ monitoring and alerts"
  echo "  □ Create DLQ investigation runbook"
  echo "  □ Implement DLQ replay mechanism"
  echo "  □ Regular DLQ review process"
  echo ""
}

trap cleanup EXIT

main "$@"
