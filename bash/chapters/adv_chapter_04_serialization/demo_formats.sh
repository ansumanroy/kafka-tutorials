#!/usr/bin/env bash
#
# demo_formats.sh
#
# Demonstrate different serialization formats in Kafka.
# Shows String, JSON, and structured data handling.
#
# Usage:
#   ./demo_formats.sh [topic_name]
#
# Examples:
#   ./demo_formats.sh                    # Use default topic
#   ./demo_formats.sh serialization-demo # Custom topic
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

# Configuration
TOPIC_NAME="${1:-serialization-demo}"
NUM_PARTITIONS=3
REPLICATION_FACTOR=1

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

create_topic() {
  log_info "Creating topic: $TOPIC_NAME"
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  
  sleep 2
  
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create \
    --topic "$TOPIC_NAME" \
    --partitions "$NUM_PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR"
  
  sleep 2
  log_success "Topic created"
}

demo_string_format() {
  log_demo "Demo 1: String Serialization (Simplest)"
  
  echo "Sending simple string messages..."
  
  cat << 'EOF' | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    2>/dev/null
Hello, Kafka!
This is a simple text message
No structure, just plain strings
EOF
  
  sleep 2
  
  log_info "Consuming messages..."
  timeout 3 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --from-beginning \
    2>/dev/null || true
  
  echo ""
  echo "Pros:"
  echo "  ✓ Simple - no configuration needed"
  echo "  ✓ Human-readable"
  echo "  ✓ Universal compatibility"
  echo ""
  echo "Cons:"
  echo "  ✗ No structure or validation"
  echo "  ✗ Difficult to parse programmatically"
  echo "  ✗ No type safety"
}

demo_json_format() {
  log_demo "Demo 2: JSON Serialization (Structured)"
  
  # Clean slate
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  sleep 2
  create_topic >/dev/null 2>&1
  
  echo "Sending structured JSON messages..."
  
  cat << 'EOF' | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    2>/dev/null
{"userId":"user-123","eventType":"login","timestamp":1640000000,"metadata":{"ip":"192.168.1.1","device":"mobile"}}
{"userId":"user-456","eventType":"purchase","timestamp":1640000100,"amount":99.99,"items":[{"id":"item-1","qty":2}]}
{"userId":"user-789","eventType":"logout","timestamp":1640000200}
EOF
  
  sleep 2
  
  log_info "Consuming and pretty-printing JSON..."
  
  if command -v jq &> /dev/null; then
    timeout 3 kafka-console-consumer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$TOPIC_NAME" \
      --from-beginning \
      2>/dev/null | jq '.' || true
  else
    timeout 3 kafka-console-consumer.sh \
      --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
      --topic "$TOPIC_NAME" \
      --from-beginning \
      2>/dev/null || true
    log_warn "Install 'jq' for pretty-printed JSON"
  fi
  
  echo ""
  echo "Pros:"
  echo "  ✓ Structured data with fields"
  echo "  ✓ Human-readable (with jq)"
  echo "  ✓ Flexible schema"
  echo "  ✓ Language-agnostic"
  echo ""
  echo "Cons:"
  echo "  ✗ Verbose (larger message size)"
  echo "  ✗ No built-in validation"
  echo "  ✗ Parsing overhead"
  echo "  ✗ Type ambiguity (everything is string/number)"
}

demo_json_validation() {
  log_demo "Demo 3: JSON Without Validation (Problem)"
  
  # Clean slate
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  sleep 2
  create_topic >/dev/null 2>&1
  
  echo "Sending both valid and invalid JSON..."
  
  cat << 'EOF' | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    2>/dev/null
{"userId":"user-001","eventType":"login","timestamp":1640000000}
{this is not valid json!!!}
{"userId":"user-002","eventType":"purchase"}
{"missing":"timestamp","wrong":"schema"}
EOF
  
  sleep 2
  
  log_warn "⚠️  All messages accepted! No validation at producer."
  
  log_info "Consumers will fail when parsing invalid JSON..."
  
  timeout 3 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --from-beginning \
    2>/dev/null || true
  
  echo ""
  log_error "Problem: Invalid JSON made it into Kafka!"
  echo ""
  echo "Solutions:"
  echo "  1. Add application-level validation before producing"
  echo "  2. Use JSON Schema validation library"
  echo "  3. Switch to Avro/Protobuf with Schema Registry"
  echo "  4. Implement consumer error handling for bad data"
}

demo_key_value_serialization() {
  log_demo "Demo 4: Key-Value Serialization"
  
  # Clean slate
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  sleep 2
  create_topic >/dev/null 2>&1
  
  echo "Sending messages with keys and values..."
  echo "(Both can have different serialization formats)"
  
  cat << 'EOF' | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --property "parse.key=true" \
    --property "key.separator=|" \
    2>/dev/null
user-123|{"eventType":"login","timestamp":1640000000}
user-456|{"eventType":"purchase","amount":99.99}
user-789|{"eventType":"logout","timestamp":1640000200}
EOF
  
  sleep 2
  
  log_info "Consuming with keys displayed..."
  
  timeout 3 kafka-console-consumer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    --from-beginning \
    --property print.key=true \
    --property key.separator=" => " \
    2>/dev/null || true
  
  echo ""
  echo "Key insights:"
  echo "  • Key serializer: StringSerializer (simple user ID)"
  echo "  • Value serializer: StringSerializer (JSON string)"
  echo "  • Can use different serializers for key vs value"
  echo "  • Example: String key + Avro value"
}

demo_message_sizes() {
  log_demo "Demo 5: Message Size Comparison"
  
  echo "Comparing message sizes for same data..."
  echo ""
  
  # Sample data
  local data_fields="userId=user-12345, eventType=purchase, amount=99.99, timestamp=1640000000000"
  
  echo "Data to encode: $data_fields"
  echo ""
  
  # String format (CSV-like)
  local string_msg="user-12345,purchase,99.99,1640000000000"
  local string_size=$(echo -n "$string_msg" | wc -c | tr -d ' ')
  echo "String/CSV format:"
  echo "  Message: $string_msg"
  echo "  Size: $string_size bytes"
  echo ""
  
  # JSON format (compact)
  local json_msg='{"userId":"user-12345","eventType":"purchase","amount":99.99,"timestamp":1640000000000}'
  local json_size=$(echo -n "$json_msg" | wc -c | tr -d ' ')
  echo "JSON format (compact):"
  echo "  Message: $json_msg"
  echo "  Size: $json_size bytes"
  echo ""
  
  # JSON format (pretty)
  local json_pretty=$(cat << 'EOF'
{
  "userId": "user-12345",
  "eventType": "purchase",
  "amount": 99.99,
  "timestamp": 1640000000000
}
EOF
)
  local json_pretty_size=$(echo -n "$json_pretty" | wc -c | tr -d ' ')
  echo "JSON format (formatted):"
  echo "$json_pretty"
  echo "  Size: $json_pretty_size bytes"
  echo ""
  
  # Simulated Avro size (typically 50-70% smaller)
  local avro_size=$((json_size * 35 / 100))
  echo "Avro format (binary, estimated):"
  echo "  Message: [binary data, not human readable]"
  echo "  Size: ~$avro_size bytes"
  echo ""
  
  # Summary
  echo "Size comparison:"
  printf "  String/CSV:     %3d bytes (baseline)\n" "$string_size"
  printf "  JSON compact:   %3d bytes (%dx larger)\n" "$json_size" $((json_size * 100 / string_size))
  printf "  JSON formatted: %3d bytes (%dx larger)\n" "$json_pretty_size" $((json_pretty_size * 100 / string_size))
  printf "  Avro binary:    %3d bytes (~%d%% of JSON)\n" "$avro_size" $((avro_size * 100 / json_size))
  
  echo ""
  echo "At 1 million messages/day:"
  printf "  String/CSV:     %.2f MB/day\n" "$(echo "scale=2; $string_size * 1000000 / 1024 / 1024" | bc)"
  printf "  JSON compact:   %.2f MB/day\n" "$(echo "scale=2; $json_size * 1000000 / 1024 / 1024" | bc)"
  printf "  JSON formatted: %.2f MB/day\n" "$(echo "scale=2; $json_pretty_size * 1000000 / 1024 / 1024" | bc)"
  printf "  Avro binary:    %.2f MB/day\n" "$(echo "scale=2; $avro_size * 1000000 / 1024 / 1024" | bc)"
}

demo_headers() {
  log_demo "Demo 6: Message Headers (Metadata)"
  
  # Clean slate
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
  sleep 2
  create_topic >/dev/null 2>&1
  
  echo "Sending messages with headers (metadata)..."
  echo ""
  echo "Headers can include:"
  echo "  • Format version (format=json-v2)"
  echo "  • Content type (content-type=application/json)"
  echo "  • Correlation ID for tracing"
  echo "  • Schema version"
  echo "  • Source system"
  echo ""
  
  # Note: kafka-console-producer doesn't support headers easily
  # This is more common in programmatic producers
  
  cat << 'EOF' | kafka-console-producer.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --topic "$TOPIC_NAME" \
    2>/dev/null
{"userId":"user-123","eventType":"login"}
EOF
  
  log_info "In practice, headers are set programmatically:"
  
  cat << 'EOF'
  
Java example:
  ProducerRecord<String, String> record = 
    new ProducerRecord<>("topic", "key", "value");
  record.headers()
    .add("format", "json-v2".getBytes())
    .add("schema-version", "2.0".getBytes())
    .add("correlation-id", "abc-123".getBytes());
  producer.send(record);

Python example:
  producer.send(
    'topic',
    key='key',
    value='value',
    headers=[
      ('format', b'json-v2'),
      ('schema-version', b'2.0'),
      ('correlation-id', b'abc-123')
    ]
  )
EOF
}

cleanup_topic() {
  log_info "Cleaning up demo topic"
  kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --delete --topic "$TOPIC_NAME" \
    --if-exists 2>/dev/null || true
}

main() {
  log_section "Kafka Serialization Formats Demonstration"
  
  log_info "Configuration:"
  echo "  Topic:        $TOPIC_NAME"
  echo "  Partitions:   $NUM_PARTITIONS"
  echo "  Bootstrap:    $KAFKA_BOOTSTRAP_SERVERS"
  
  # Setup
  create_topic
  
  # Run demos
  demo_string_format
  demo_json_format
  demo_json_validation
  demo_key_value_serialization
  demo_message_sizes
  demo_headers
  
  # Cleanup
  cleanup_topic
  
  log_section "Demonstration Complete"
  log_success "All serialization formats demonstrated"
  
  echo ""
  echo "Key Takeaways:"
  echo "  1. String: Simple but unstructured"
  echo "  2. JSON: Structured but verbose, no validation"
  echo "  3. Binary (Avro/Protobuf): Compact, validated, needs Schema Registry"
  echo "  4. Choose based on: size, validation needs, debugging requirements"
  echo ""
  echo "Recommendations:"
  echo "  • Development/Debugging: JSON (human-readable)"
  echo "  • Production/Scale: Avro or Protobuf with Schema Registry"
  echo "  • Always validate data before producing"
  echo "  • Use compression for large messages"
  echo ""
}

trap cleanup_topic EXIT

main "$@"
