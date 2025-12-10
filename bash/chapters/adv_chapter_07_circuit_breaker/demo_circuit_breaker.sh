#!/bin/bash

# Demo: Circuit Breaker Pattern
# Shows state transitions: CLOSED → OPEN → HALF-OPEN → CLOSED

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/circuit_breaker.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "╔════════════════════════════════════════════════════════╗"
echo "║    Circuit Breaker Pattern Demo                       ║"
echo "╔════════════════════════════════════════════════════════╗"
echo ""

# Reset circuit breaker
rm -f /tmp/circuit_breaker_state
init_circuit_breaker

echo "Configuration:"
echo "  Failure Threshold: $CB_FAILURE_THRESHOLD"
echo "  Success Threshold: $CB_SUCCESS_THRESHOLD"
echo "  Timeout: ${CB_TIMEOUT}s"
echo ""

# Phase 1: Normal operation (CLOSED)
echo -e "${BLUE}=== Phase 1: Normal Operation (CLOSED) ===${NC}"
echo ""

for i in {1..3}; do
  echo "Request #$i:"
  
  if circuit_breaker_allow_request; then
    echo -e "  ${GREEN}✅ Request ALLOWED${NC}"
    echo "  → Simulating successful Kafka send..."
    sleep 0.5
    circuit_breaker_record_success
  else
    echo -e "  ${RED}❌ Request REJECTED${NC}"
  fi
  
  circuit_breaker_status
  echo ""
done

# Phase 2: Failures causing circuit to open
echo -e "${YELLOW}=== Phase 2: Consecutive Failures ===${NC}"
echo "Simulating Kafka outage..."
echo ""

for i in {1..6}; do
  echo "Request #$((i + 3)):"
  
  if circuit_breaker_allow_request; then
    echo -e "  ${YELLOW}⚠️  Request ALLOWED (trying...)${NC}"
    echo "  → Simulating failed Kafka send..."
    sleep 0.5
    circuit_breaker_record_failure
  else
    echo -e "  ${RED}🔴 Request REJECTED (circuit is OPEN)${NC}"
    echo "  → Failing fast (no Kafka call)"
  fi
  
  circuit_breaker_status
  echo ""
done

# Phase 3: Wait for timeout
echo -e "${YELLOW}=== Phase 3: Waiting for Timeout ===${NC}"
echo "Circuit is OPEN. Waiting for timeout to transition to HALF-OPEN..."
echo ""

# Simulate some rejected requests while waiting
for i in {1..3}; do
  echo "Request #$((i + 9)):"
  
  if circuit_breaker_allow_request; then
    echo -e "  ${GREEN}Request ALLOWED${NC}"
  else
    echo -e "  ${RED}🔴 Request REJECTED (circuit is OPEN)${NC}"
  fi
  
  circuit_breaker_status
  echo ""
  sleep 1
done

echo "Simulating passage of time..."
# Manually set open time to make timeout expire
CB_OPEN_TIME=$(($(date +%s) - CB_TIMEOUT - 1))
save_circuit_breaker_state
echo ""

# Phase 4: Half-open state
echo -e "${BLUE}=== Phase 4: Testing with HALF-OPEN ===${NC}"
echo "Circuit should transition to HALF-OPEN..."
echo ""

for i in {1..3}; do
  echo "Request #$((i + 12)):"
  
  if circuit_breaker_allow_request; then
    echo -e "  ${GREEN}✅ Request ALLOWED (testing...)${NC}"
    echo "  → Simulating successful Kafka send..."
    sleep 0.5
    circuit_breaker_record_success
  else
    echo -e "  ${RED}Request REJECTED${NC}"
  fi
  
  circuit_breaker_status
  echo ""
done

# Phase 5: Circuit closed again
echo -e "${GREEN}=== Phase 5: Circuit Restored (CLOSED) ===${NC}"
echo ""

for i in {1..2}; do
  echo "Request #$((i + 15)):"
  
  if circuit_breaker_allow_request; then
    echo -e "  ${GREEN}✅ Request ALLOWED${NC}"
    echo "  → Simulating successful Kafka send..."
    sleep 0.5
    circuit_breaker_record_success
  else
    echo -e "  ${RED}Request REJECTED${NC}"
  fi
  
  circuit_breaker_status
  echo ""
done

echo "╔════════════════════════════════════════════════════════╗"
echo "║    Demo Complete!                                      ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
echo "Summary of state transitions:"
echo -e "  ${BLUE}CLOSED${NC} → ${RED}OPEN${NC} → ${YELLOW}HALF-OPEN${NC} → ${GREEN}CLOSED${NC}"
echo ""
echo "Key takeaways:"
echo "  • Circuit opens after $CB_FAILURE_THRESHOLD consecutive failures"
echo "  • Requests are rejected immediately when circuit is OPEN"
echo "  • After ${CB_TIMEOUT}s timeout, circuit tries HALF-OPEN state"
echo "  • Circuit closes after $CB_SUCCESS_THRESHOLD successes in HALF-OPEN"
