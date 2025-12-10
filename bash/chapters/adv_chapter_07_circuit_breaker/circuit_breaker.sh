#!/bin/bash

# Circuit Breaker Implementation for Kafka Producer
# Provides fail-fast behavior and automatic recovery

# Circuit breaker state file
CB_STATE_FILE="/tmp/circuit_breaker_state"
CB_STATE="CLOSED"              # CLOSED, OPEN, HALF_OPEN
CB_FAILURE_COUNT=0
CB_SUCCESS_COUNT=0
CB_LAST_FAILURE_TIME=0
CB_OPEN_TIME=0

# Configuration (can be overridden)
CB_FAILURE_THRESHOLD=${CB_FAILURE_THRESHOLD:-5}         # Open after 5 failures
CB_SUCCESS_THRESHOLD=${CB_SUCCESS_THRESHOLD:-2}         # Close after 2 successes
CB_TIMEOUT=${CB_TIMEOUT:-60}                            # Try half-open after 60s
CB_HALF_OPEN_MAX_REQUESTS=${CB_HALF_OPEN_MAX_REQUESTS:-3}
CB_RESET_TIMEOUT=${CB_RESET_TIMEOUT:-30}                # Reset failure count after 30s

# Initialize circuit breaker
init_circuit_breaker() {
  if [ -f "$CB_STATE_FILE" ]; then
    source "$CB_STATE_FILE"
  else
    save_circuit_breaker_state
  fi
}

# Save state to file
save_circuit_breaker_state() {
  cat > "$CB_STATE_FILE" << EOF
CB_STATE="$CB_STATE"
CB_FAILURE_COUNT=$CB_FAILURE_COUNT
CB_SUCCESS_COUNT=$CB_SUCCESS_COUNT
CB_LAST_FAILURE_TIME=$CB_LAST_FAILURE_TIME
CB_OPEN_TIME=$CB_OPEN_TIME
EOF
}

# Check if request should be allowed
circuit_breaker_allow_request() {
  local current_time=$(date +%s)
  
  case "$CB_STATE" in
    CLOSED)
      # Check if we should reset failure count
      if [ $CB_FAILURE_COUNT -gt 0 ] && [ $CB_LAST_FAILURE_TIME -gt 0 ]; then
        local time_since_failure=$((current_time - CB_LAST_FAILURE_TIME))
        if [ $time_since_failure -ge $CB_RESET_TIMEOUT ]; then
          CB_FAILURE_COUNT=0
          save_circuit_breaker_state
        fi
      fi
      return 0  # Allow request
      ;;
      
    OPEN)
      # Check if timeout expired
      local time_since_open=$((current_time - CB_OPEN_TIME))
      if [ $time_since_open -ge $CB_TIMEOUT ]; then
        echo "⚡ Circuit transitioning to HALF-OPEN (timeout expired after ${time_since_open}s)"
        CB_STATE="HALF_OPEN"
        CB_SUCCESS_COUNT=0
        CB_FAILURE_COUNT=0
        save_circuit_breaker_state
        return 0  # Allow test request
      fi
      return 1  # Reject request
      ;;
      
    HALF_OPEN)
      return 0  # Allow limited requests
      ;;
  esac
}

# Record successful request
circuit_breaker_record_success() {
  case "$CB_STATE" in
    CLOSED)
      CB_FAILURE_COUNT=0  # Reset failure count
      CB_LAST_FAILURE_TIME=0
      ;;
      
    HALF_OPEN)
      CB_SUCCESS_COUNT=$((CB_SUCCESS_COUNT + 1))
      echo "✅ Success in HALF-OPEN ($CB_SUCCESS_COUNT/$CB_SUCCESS_THRESHOLD)"
      
      if [ $CB_SUCCESS_COUNT -ge $CB_SUCCESS_THRESHOLD ]; then
        echo "🔵 Circuit CLOSING (success threshold met)"
        CB_STATE="CLOSED"
        CB_FAILURE_COUNT=0
        CB_SUCCESS_COUNT=0
      fi
      ;;
  esac
  
  save_circuit_breaker_state
}

# Record failed request
circuit_breaker_record_failure() {
  local current_time=$(date +%s)
  
  case "$CB_STATE" in
    CLOSED)
      CB_FAILURE_COUNT=$((CB_FAILURE_COUNT + 1))
      CB_LAST_FAILURE_TIME=$current_time
      
      echo "❌ Failure in CLOSED ($CB_FAILURE_COUNT/$CB_FAILURE_THRESHOLD)"
      
      if [ $CB_FAILURE_COUNT -ge $CB_FAILURE_THRESHOLD ]; then
        echo "🔴 Circuit OPENING (failure threshold exceeded)"
        CB_STATE="OPEN"
        CB_OPEN_TIME=$current_time
      fi
      ;;
      
    HALF_OPEN)
      echo "❌ Failure in HALF-OPEN - reopening circuit"
      CB_STATE="OPEN"
      CB_OPEN_TIME=$current_time
      CB_FAILURE_COUNT=0
      CB_SUCCESS_COUNT=0
      ;;
      
    OPEN)
      # Already open, just update time
      CB_OPEN_TIME=$current_time
      ;;
  esac
  
  save_circuit_breaker_state
}

# Get current state
circuit_breaker_status() {
  echo "Circuit Breaker Status:"
  echo "  State: $CB_STATE"
  echo "  Failure Count: $CB_FAILURE_COUNT"
  echo "  Success Count: $CB_SUCCESS_COUNT"
  
  if [ "$CB_STATE" = "OPEN" ]; then
    local current_time=$(date +%s)
    local time_since_open=$((current_time - CB_OPEN_TIME))
    local time_remaining=$((CB_TIMEOUT - time_since_open))
    if [ $time_remaining -gt 0 ]; then
      echo "  Time until half-open: ${time_remaining}s"
    else
      echo "  Ready to transition to half-open"
    fi
  fi
}

# Reset circuit breaker (for testing)
circuit_breaker_reset() {
  CB_STATE="CLOSED"
  CB_FAILURE_COUNT=0
  CB_SUCCESS_COUNT=0
  CB_LAST_FAILURE_TIME=0
  CB_OPEN_TIME=0
  save_circuit_breaker_state
  echo "Circuit breaker reset to CLOSED state"
}

# Export functions
export -f init_circuit_breaker
export -f save_circuit_breaker_state
export -f circuit_breaker_allow_request
export -f circuit_breaker_record_success
export -f circuit_breaker_record_failure
export -f circuit_breaker_status
export -f circuit_breaker_reset
