#!/usr/bin/env bash
#
# analyze_distribution.sh
#
# Analyze key distribution across partitions for an existing topic.
# Helps identify hot partitions and uneven load distribution.
#
# Usage:
#   ./analyze_distribution.sh <topic_name>
#
# Examples:
#   ./analyze_distribution.sh my-topic
#   ./analyze_distribution.sh user-events
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
TOPIC_NAME="${1:-}"

log_section() {
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "════════════════════════════════════════════════════════════"
}

usage() {
  cat << EOF
Usage: $0 <topic_name>

Analyze partition distribution for a Kafka topic.

Arguments:
  topic_name    Name of the topic to analyze (required)

Examples:
  $0 my-topic
  $0 user-events

EOF
  exit 1
}

check_topic_exists() {
  local exists
  exists=$(kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --list 2>/dev/null | grep -c "^${TOPIC_NAME}$" || echo "0")
  
  if [ "$exists" -eq 0 ]; then
    log_error "Topic '$TOPIC_NAME' does not exist"
    echo ""
    echo "Available topics:"
    kafka-topics.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --list 2>/dev/null
    exit 1
  fi
}

get_topic_info() {
  log_section "Topic Information"
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --describe --topic "$TOPIC_NAME" 2>/dev/null
  
  echo ""
}

analyze_partition_offsets() {
  log_section "Partition Offset Analysis"
  
  log_info "Fetching partition offsets..."
  
  local offsets
  offsets=$(kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --time -1 2>/dev/null)
  
  if [ -z "$offsets" ]; then
    log_error "Could not fetch offsets for topic $TOPIC_NAME"
    return 1
  fi
  
  echo ""
  echo "Message Distribution:"
  echo "─────────────────────────────────────────────────"
  
  local total=0
  local max_offset=0
  local min_offset=999999999
  declare -A partition_offsets
  local num_partitions=0
  
  # Parse offsets
  while IFS=: read -r topic partition offset; do
    partition_offsets[$partition]=$offset
    total=$((total + offset))
    num_partitions=$((num_partitions + 1))
    [ $offset -gt $max_offset ] && max_offset=$offset
    [ $offset -lt $min_offset ] && min_offset=$offset
  done <<< "$offsets"
  
  # Display distribution with visualization
  for partition in $(seq 0 $((num_partitions - 1))); do
    local offset=${partition_offsets[$partition]:-0}
    
    if [ $total -gt 0 ]; then
      local percent=$(echo "scale=1; $offset * 100 / $total" | bc 2>/dev/null || echo "0")
    else
      local percent="0.0"
    fi
    
    # Create bar chart
    local bar_length=0
    if [ $max_offset -gt 0 ]; then
      bar_length=$(echo "scale=0; $offset * 50 / $max_offset" | bc 2>/dev/null || echo "0")
    fi
    local bar=$(printf '█%.0s' $(seq 1 $bar_length))
    
    # Highlight hot partitions
    local marker=""
    if [ $max_offset -gt 0 ] && [ $offset -eq $max_offset ] && [ $num_partitions -gt 1 ]; then
      local avg=$((total / num_partitions))
      if [ $offset -gt $((avg * 150 / 100)) ]; then
        marker=" ⚠️ HOT"
      fi
    fi
    
    printf "Partition %2d: %10d messages (%5.1f%%) %s%s\n" \
      "$partition" "$offset" "$percent" "$bar" "$marker"
  done
  
  echo ""
  echo "Statistics:"
  echo "─────────────────────────────────────────────────"
  
  local avg=0
  [ $num_partitions -gt 0 ] && avg=$((total / num_partitions))
  
  echo "  Total messages:        $total"
  echo "  Number of partitions:  $num_partitions"
  echo "  Average per partition: $avg"
  echo "  Maximum partition:     $max_offset"
  echo "  Minimum partition:     $min_offset"
  echo "  Variance:              $((max_offset - min_offset))"
  
  if [ $avg -gt 0 ]; then
    local max_ratio=$(echo "scale=2; $max_offset / $avg" | bc 2>/dev/null || echo "1.0")
    local min_ratio=$(echo "scale=2; $min_offset / $avg" | bc 2>/dev/null || echo "1.0")
    echo "  Max/Avg ratio:         ${max_ratio}x"
    echo "  Min/Avg ratio:         ${min_ratio}x"
    
    # Calculate standard deviation
    local variance_sum=0
    for partition in $(seq 0 $((num_partitions - 1))); do
      local offset=${partition_offsets[$partition]:-0}
      local diff=$((offset - avg))
      local diff_sq=$((diff * diff))
      variance_sum=$((variance_sum + diff_sq))
    done
    local variance=$((variance_sum / num_partitions))
    local std_dev=$(echo "scale=0; sqrt($variance)" | bc 2>/dev/null || echo "0")
    local coeff_var=$(echo "scale=2; $std_dev / $avg" | bc 2>/dev/null || echo "0")
    
    echo "  Standard deviation:    $std_dev"
    echo "  Coefficient of var:    $coeff_var"
  fi
  
  echo ""
  
  # Health assessment
  if [ $num_partitions -gt 1 ] && [ $avg -gt 0 ]; then
    local max_ratio=$(echo "scale=0; $max_offset * 100 / $avg" | bc 2>/dev/null || echo "100")
    
    if [ "$max_ratio" -lt 120 ]; then
      log_success "✓ Distribution is well-balanced"
    elif [ "$max_ratio" -lt 150 ]; then
      log_warn "⚠ Distribution has some skew (max is ${max_ratio}% of average)"
    else
      log_error "⚠ Distribution is heavily skewed (max is ${max_ratio}% of average)"
      echo "  Consider reviewing your partition key strategy."
    fi
  fi
}

analyze_partition_sizes() {
  log_section "Partition Size Analysis"
  
  log_info "Fetching partition sizes (this may take a moment)..."
  
  local sizes
  sizes=$(kafka-log-dirs.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic-list "$TOPIC_NAME" \
    --describe 2>/dev/null | \
    grep -oP '"partition":\d+.*?"size":\d+' | \
    sed 's/"partition"://g; s/"size"://g; s/[^0-9 ]//g' || echo "")
  
  if [ -z "$sizes" ]; then
    log_warn "Could not fetch partition sizes"
    return 0
  fi
  
  echo ""
  echo "Disk Usage per Partition:"
  echo "─────────────────────────────────────────────────"
  
  local total_size=0
  local max_size=0
  declare -A partition_sizes
  
  while read -r partition size; do
    partition_sizes[$partition]=$size
    total_size=$((total_size + size))
    [ $size -gt $max_size ] && max_size=$size
  done <<< "$sizes"
  
  for partition in "${!partition_sizes[@]}"; do
    local size=${partition_sizes[$partition]}
    local size_mb=$(echo "scale=2; $size / 1024 / 1024" | bc 2>/dev/null || echo "0")
    
    local bar_length=0
    if [ $max_size -gt 0 ]; then
      bar_length=$(echo "scale=0; $size * 50 / $max_size" | bc 2>/dev/null || echo "0")
    fi
    local bar=$(printf '█%.0s' $(seq 1 $bar_length))
    
    printf "Partition %2d: %8.2f MB %s\n" "$partition" "$size_mb" "$bar"
  done | sort -n -k2
  
  echo ""
  local total_mb=$(echo "scale=2; $total_size / 1024 / 1024" | bc 2>/dev/null || echo "0")
  echo "Total disk usage: ${total_mb} MB"
}

analyze_consumer_groups() {
  log_section "Consumer Group Analysis"
  
  log_info "Finding consumer groups for topic $TOPIC_NAME..."
  
  local groups
  groups=$(kafka-consumer-groups.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --list 2>/dev/null || echo "")
  
  if [ -z "$groups" ]; then
    log_warn "No consumer groups found"
    return 0
  fi
  
  local found_groups=0
  
  for group in $groups; do
    local group_topics
    group_topics=$(kafka-consumer-groups.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --group "$group" \
      --describe 2>/dev/null | grep "$TOPIC_NAME" || echo "")
    
    if [ -n "$group_topics" ]; then
      found_groups=$((found_groups + 1))
      
      echo ""
      echo "Consumer Group: $group"
      echo "─────────────────────────────────────────────────"
      kafka-consumer-groups.sh \
        --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --group "$group" \
        --describe 2>/dev/null | grep -E "(TOPIC|${TOPIC_NAME})"
      
      # Check for lag issues
      local max_lag
      max_lag=$(echo "$group_topics" | awk '{print $6}' | grep -E '^[0-9]+$' | sort -n | tail -1 || echo "0")
      
      if [ "$max_lag" -gt 1000 ]; then
        echo ""
        log_warn "⚠ High lag detected: $max_lag messages"
        echo "  This may indicate a hot partition or slow consumer."
      fi
    fi
  done
  
  if [ $found_groups -eq 0 ]; then
    echo "No consumer groups currently consuming from $TOPIC_NAME"
  fi
}

recommendations() {
  log_section "Recommendations"
  
  echo "Based on the analysis:"
  echo ""
  echo "1. KEY SELECTION:"
  echo "   • Use high-cardinality fields (user_id, device_id, order_id)"
  echo "   • Avoid low-cardinality fields (country, status, type)"
  echo "   • Consider composite keys for hot entities"
  echo ""
  echo "2. MONITORING:"
  echo "   • Set alerts for partition lag > threshold"
  echo "   • Monitor partition offset growth rate"
  echo "   • Track disk usage per partition"
  echo ""
  echo "3. OPTIMIZATION:"
  echo "   • If hot partition detected: shard the hot key"
  echo "   • If uneven sizes: review message size distribution"
  echo "   • If consumer lag: scale consumers or optimize processing"
  echo ""
  echo "4. PARTITION COUNT:"
  echo "   • Ensure partitions >= number of consumers"
  echo "   • Plan for future growth (2-3x current load)"
  echo "   • ⚠ Changing partition count breaks key-to-partition mapping!"
  echo ""
}

main() {
  # Validate arguments
  if [ -z "$TOPIC_NAME" ]; then
    usage
  fi
  
  log_section "Partition Distribution Analysis"
  echo "Topic: $TOPIC_NAME"
  echo "Bootstrap: $KAFKA_BOOTSTRAP_SERVERS"
  
  # Run analysis
  check_topic_exists
  get_topic_info
  analyze_partition_offsets
  analyze_partition_sizes
  analyze_consumer_groups
  recommendations
  
  log_section "Analysis Complete"
}

main "$@"
