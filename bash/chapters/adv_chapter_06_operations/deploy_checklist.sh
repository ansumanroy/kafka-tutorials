#!/usr/bin/env bash
#
# deploy_checklist.sh
#
# Interactive deployment checklist for Kafka producer applications.
# Ensures all operational concerns are addressed before production deployment.
#
# Usage:
#   ./deploy_checklist.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_section() {
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "════════════════════════════════════════════════════════════"
}

print_checklist_section() {
  echo ""
  echo "────────────────────────────────────────────────────────────"
  echo "  $1"
  echo "────────────────────────────────────────────────────────────"
}

ask_yes_no() {
  local question=$1
  local answer
  
  while true; do
    read -p "$question (y/n): " answer
    case $answer in
      [Yy]* ) return 0;;
      [Nn]* ) return 1;;
      * ) echo "Please answer yes or no.";;
    esac
  done
}

checklist_configuration() {
  print_checklist_section "1. Configuration Review"
  
  local passed=0
  local total=0
  
  # Idempotence
  total=$((total + 1))
  if ask_yes_no "  □ Is enable.idempotence=true configured?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Idempotence enabled"
  else
    echo -e "    ${RED}✗${NC} WARNING: Idempotence should be enabled for reliability"
  fi
  
  # Acks
  total=$((total + 1))
  if ask_yes_no "  □ Is acks=all configured?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Acks=all configured"
  else
    echo -e "    ${RED}✗${NC} WARNING: acks=all recommended for data safety"
  fi
  
  # Compression
  total=$((total + 1))
  if ask_yes_no "  □ Is compression enabled (lz4/zstd)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Compression enabled"
  else
    echo -e "    ${YELLOW}⚠${NC}  Consider enabling compression for efficiency"
  fi
  
  # Batch size
  total=$((total + 1))
  if ask_yes_no "  □ Is batch.size tuned for your workload?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Batch size configured"
  else
    echo -e "    ${YELLOW}⚠${NC}  Review batch.size (default: 16KB, consider 32-64KB)"
  fi
  
  # Linger
  total=$((total + 1))
  if ask_yes_no "  □ Is linger.ms configured (10-20ms for throughput)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Linger configured"
  else
    echo -e "    ${YELLOW}⚠${NC}  Consider small linger.ms (10-20ms) for better batching"
  fi
  
  echo ""
  echo "Configuration Score: $passed/$total"
  
  return 0
}

checklist_security() {
  print_checklist_section "2. Security Configuration"
  
  local passed=0
  local total=0
  
  # Security protocol
  total=$((total + 1))
  if ask_yes_no "  □ Is security.protocol=SASL_SSL configured?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Secure protocol enabled"
  else
    echo -e "    ${RED}✗${NC} CRITICAL: SASL_SSL required for production"
  fi
  
  # SASL mechanism
  total=$((total + 1))
  if ask_yes_no "  □ Is SASL mechanism configured (SCRAM/IAM)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} SASL mechanism configured"
  else
    echo -e "    ${RED}✗${NC} CRITICAL: SASL authentication required"
  fi
  
  # Credentials
  total=$((total + 1))
  if ask_yes_no "  □ Are credentials stored in secret manager (not hardcoded)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Credentials secured"
  else
    echo -e "    ${RED}✗${NC} CRITICAL: Never hardcode credentials"
  fi
  
  # TLS certificates
  total=$((total + 1))
  if ask_yes_no "  □ Are TLS certificates valid and not expiring soon?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Certificates valid"
  else
    echo -e "    ${RED}✗${NC} WARNING: Check certificate expiration"
  fi
  
  # ACLs
  total=$((total + 1))
  if ask_yes_no "  □ Are ACLs configured with least privilege?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} ACLs configured"
  else
    echo -e "    ${YELLOW}⚠${NC}  Review ACL permissions"
  fi
  
  echo ""
  echo "Security Score: $passed/$total"
  
  if [ $passed -lt 3 ]; then
    echo -e "${RED}✗ CRITICAL: Security requirements not met${NC}"
    return 1
  fi
  
  return 0
}

checklist_monitoring() {
  print_checklist_section "3. Monitoring & Observability"
  
  local passed=0
  local total=0
  
  # Metrics export
  total=$((total + 1))
  if ask_yes_no "  □ Are producer metrics exported (JMX/Prometheus)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Metrics exported"
  else
    echo -e "    ${RED}✗${NC} WARNING: Metrics are essential for operations"
  fi
  
  # Dashboards
  total=$((total + 1))
  if ask_yes_no "  □ Are Grafana dashboards created for producer metrics?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Dashboards created"
  else
    echo -e "    ${YELLOW}⚠${NC}  Create dashboards for visibility"
  fi
  
  # Alerts
  total=$((total + 1))
  if ask_yes_no "  □ Are alerts configured (error rate, latency, DLQ)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Alerts configured"
  else
    echo -e "    ${RED}✗${NC} WARNING: Alerts critical for incident response"
  fi
  
  # DLQ
  total=$((total + 1))
  if ask_yes_no "  □ Is Dead Letter Queue created and monitored?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} DLQ configured"
  else
    echo -e "    ${YELLOW}⚠${NC}  DLQ recommended for error handling"
  fi
  
  # Logging
  total=$((total + 1))
  if ask_yes_no "  □ Is structured logging configured (JSON)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Structured logging enabled"
  else
    echo -e "    ${YELLOW}⚠${NC}  Structured logging improves troubleshooting"
  fi
  
  # Tracing
  total=$((total + 1))
  if ask_yes_no "  □ Is distributed tracing enabled (OpenTelemetry)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Tracing enabled"
  else
    echo -e "    ${YELLOW}⚠${NC}  Tracing helps with request flow analysis"
  fi
  
  echo ""
  echo "Monitoring Score: $passed/$total"
  
  return 0
}

checklist_reliability() {
  print_checklist_section "4. Reliability & Error Handling"
  
  local passed=0
  local total=0
  
  # Error handling
  total=$((total + 1))
  if ask_yes_no "  □ Is error handling implemented (callbacks/try-catch)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Error handling implemented"
  else
    echo -e "    ${RED}✗${NC} CRITICAL: Error handling required"
  fi
  
  # Graceful shutdown
  total=$((total + 1))
  if ask_yes_no "  □ Is graceful shutdown implemented (SIGTERM handling)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Graceful shutdown implemented"
  else
    echo -e "    ${RED}✗${NC} WARNING: Graceful shutdown prevents data loss"
  fi
  
  # Retries
  total=$((total + 1))
  if ask_yes_no "  □ Are retry settings configured appropriately?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Retries configured"
  else
    echo -e "    ${YELLOW}⚠${NC}  Review retry configuration"
  fi
  
  # Timeouts
  total=$((total + 1))
  if ask_yes_no "  □ Are timeouts set (delivery.timeout.ms, request.timeout.ms)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Timeouts configured"
  else
    echo -e "    ${YELLOW}⚠${NC}  Set appropriate timeouts for your network"
  fi
  
  echo ""
  echo "Reliability Score: $passed/$total"
  
  if [ $passed -lt 2 ]; then
    echo -e "${RED}✗ CRITICAL: Reliability requirements not met${NC}"
    return 1
  fi
  
  return 0
}

checklist_operations() {
  print_checklist_section "5. Operational Readiness"
  
  local passed=0
  local total=0
  
  # Testing
  total=$((total + 1))
  if ask_yes_no "  □ Has load testing been completed in staging?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Load testing done"
  else
    echo -e "    ${RED}✗${NC} WARNING: Load test before production"
  fi
  
  # Failure testing
  total=$((total + 1))
  if ask_yes_no "  □ Have failure scenarios been tested (network, broker down)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Failure testing done"
  else
    echo -e "    ${YELLOW}⚠${NC}  Test failure scenarios to validate reliability"
  fi
  
  # Runbooks
  total=$((total + 1))
  if ask_yes_no "  □ Are operational runbooks documented?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Runbooks documented"
  else
    echo -e "    ${YELLOW}⚠${NC}  Document common troubleshooting procedures"
  fi
  
  # Rollback plan
  total=$((total + 1))
  if ask_yes_no "  □ Is rollback plan prepared and tested?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Rollback plan ready"
  else
    echo -e "    ${RED}✗${NC} WARNING: Always have rollback plan"
  fi
  
  # Health checks
  total=$((total + 1))
  if ask_yes_no "  □ Are health checks implemented (liveness/readiness)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Health checks implemented"
  else
    echo -e "    ${YELLOW}⚠${NC}  Health checks enable automatic recovery"
  fi
  
  # Resource limits
  total=$((total + 1))
  if ask_yes_no "  □ Are resource limits set (CPU, memory, connections)?"; then
    passed=$((passed + 1))
    echo -e "    ${GREEN}✓${NC} Resource limits set"
  else
    echo -e "    ${YELLOW}⚠${NC}  Set resource limits to prevent resource exhaustion"
  fi
  
  echo ""
  echo "Operations Score: $passed/$total"
  
  return 0
}

deployment_recommendation() {
  log_section "Deployment Recommendation"
  
  echo ""
  echo "Based on your checklist responses:"
  echo ""
  
  # Check if critical items passed
  if ! checklist_security >/dev/null 2>&1 || ! checklist_reliability >/dev/null 2>&1; then
    echo -e "${RED}❌ NOT READY FOR PRODUCTION${NC}"
    echo ""
    echo "Critical issues must be resolved:"
    echo "  • Security configuration incomplete"
    echo "  • Reliability requirements not met"
    echo ""
    echo "Recommendation: DO NOT DEPLOY to production"
    return 1
  fi
  
  echo -e "${GREEN}✓ Ready for deployment${NC}"
  echo ""
  echo "Deployment steps:"
  echo "  1. Deploy to staging environment first"
  echo "  2. Run smoke tests and verify metrics"
  echo "  3. Monitor for 30+ minutes in staging"
  echo "  4. Deploy to production using canary/rolling strategy"
  echo "  5. Monitor closely for first 24-48 hours"
  echo "  6. Keep rollback plan ready"
  echo ""
  
  return 0
}

main() {
  log_section "Kafka Producer Deployment Checklist"
  
  echo ""
  echo "This interactive checklist ensures your Kafka producer"
  echo "is ready for production deployment."
  echo ""
  echo "Answer each question honestly based on your current setup."
  echo ""
  
  read -p "Press Enter to begin..."
  
  # Run checklist sections
  checklist_configuration
  checklist_security
  checklist_monitoring
  checklist_reliability
  checklist_operations
  
  # Final recommendation
  deployment_recommendation
  
  log_section "Checklist Complete"
  
  echo ""
  echo "Additional Resources:"
  echo "  • Review chapter documentation for details"
  echo "  • Test in staging before production"
  echo "  • Keep this checklist for future deployments"
  echo "  • Update checklist based on lessons learned"
  echo ""
}

main "$@"
