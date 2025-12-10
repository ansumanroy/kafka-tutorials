#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# shellcheck disable=SC1091
source "$ROOT_DIR/bash/common/env.sh"
source "$ROOT_DIR/bash/common/log.sh"

TOPIC_NAME="${1:-failure-test-topic}"

load_kafka_env
ensure_kafka_cli

info "==================================================="
info "Producer Failure Simulation Test"
info "==================================================="
info ""
info "This script demonstrates producer behavior under various"
info "failure scenarios and timeout conditions."
info ""

# Create topic
info "[Setup] Creating test topic..."
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null || true

info "[Setup] ✓ Topic ready"
echo ""

info "[Scenario 1] Short timeout - simulating request timeout..."
info "Configuration: request.timeout.ms=1000 (very short)"

# This may fail or succeed depending on network speed
{
    echo "timeout-test-message-1"
    echo "timeout-test-message-2"
} | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --producer-property request.timeout.ms=1000 \
    --producer-property delivery.timeout.ms=5000 \
    --producer-property acks=all \
    --producer-property retries=2 \
    --producer-property client.id=test-timeout 2>&1 | head -20 || warn "Some messages may have timed out (expected)"

echo ""
info "[Scenario 1] Complete - observe any timeout warnings above"
echo ""

info "[Scenario 2] Aggressive retries with backoff..."
info "Configuration: retries=5, retry.backoff.ms=500"

{
    for i in {1..3}; do
        echo "retry-message-$i"
    done
} | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --producer-property enable.idempotence=true \
    --producer-property acks=all \
    --producer-property retries=5 \
    --producer-property retry.backoff.ms=500 \
    --producer-property client.id=test-aggressive-retry 2>&1 | grep -v "^>>" || true

info "[Scenario 2] ✓ Messages sent with retry protection"
echo ""

info "[Scenario 3] Testing acks=0 (fire-and-forget, no guarantees)..."
warn "Note: acks=0 provides NO delivery guarantees!"

{
    for i in {1..5}; do
        echo "fire-and-forget-$i"
    done
} | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --producer-property acks=0 \
    --producer-property client.id=test-no-ack 2>&1 | grep -v "^>>" || true

warn "[Scenario 3] Messages sent with acks=0 - some may be lost in production!"
echo ""

info "==================================================="
info "Failure Simulation Complete"
info "==================================================="
info ""
info "Review the messages that made it through:"
echo "  bash/chapters/04-consumer-console/consume_messages.sh $TOPIC_NAME earliest"
info ""
info "Clean up test topic:"
echo "  bash/chapters/02-topics/delete_topic.sh $TOPIC_NAME --force"

