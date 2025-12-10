#!/usr/bin/env bash
#
# test_schema_evolution.sh
#
# Demonstrate schema evolution concepts and compatibility testing.
# Shows what changes are safe and which break compatibility.
#
# Usage:
#   ./test_schema_evolution.sh
#
# Note: This demonstrates concepts. Actual Avro/Schema Registry testing
#       requires additional setup (see docker-compose-full.yml).
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(cd "$SCRIPT_DIR/../../common" && pwd)"

# Source common utilities
source "$COMMON_DIR/env_loader.sh"
source "$COMMON_DIR/logging.sh"

log_section() {
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo "  $1"
  echo "════════════════════════════════════════════════════════════"
}

log_scenario() {
  echo ""
  echo "────────────────────────────────────────────────────────────"
  echo "  $1"
  echo "────────────────────────────────────────────────────────────"
}

demonstrate_schema_v1() {
  log_scenario "Scenario 1: Initial Schema (Version 1)"
  
  cat << 'EOF'
Schema Version 1 (User record):
{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"}
  ]
}

Sample data:
{
  "id": "user-123",
  "name": "Alice",
  "email": "alice@example.com"
}

✓ Schema registered
✓ Producers sending v1 messages
✓ Consumers reading v1 messages
EOF
}

demonstrate_backward_compatible() {
  log_scenario "Scenario 2: BACKWARD Compatible Change"
  
  cat << 'EOF'
Change: Add optional field with default value

Schema Version 2:
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "phone", "type": ["null", "string"], "default": null}  ← NEW
  ]
}

Compatibility Test: BACKWARD
────────────────────────────────

Producer (v2):                Consumer (v1):
Sends:                        Reads:
{                             {
  "id": "user-123",             "id": "user-123",
  "name": "Alice",              "name": "Alice",
  "email": "alice@...",         "email": "alice@..."
  "phone": "555-1234"           (ignores "phone")
}                             }

✓ OLD CONSUMERS can read NEW data
  → Simply ignore unknown "phone" field

Result: ✓ BACKWARD COMPATIBLE

Deployment strategy:
  1. Deploy new schema (v2)
  2. Old consumers still work (ignore new field)
  3. Upgrade consumers gradually
  4. Upgrade producers to send new field
EOF
}

demonstrate_forward_compatible() {
  log_scenario "Scenario 3: FORWARD Compatible Change"
  
  cat << 'EOF'
Change: Remove optional field

Schema Version 2 (Remove field):
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"}
    // "email" field removed
  ]
}

Compatibility Test: FORWARD
────────────────────────────────

Producer (v2):                Consumer (v1):
Sends:                        Expects:
{                             {
  "id": "user-123",             "id": "user-123",
  "name": "Alice"               "name": "Alice",
}                               "email": ??? ← Field missing!
                              }

✓ OLD CONSUMERS can handle missing field
  → If "email" had default value or was optional

Result: ✓ FORWARD COMPATIBLE (if field was optional)

Deployment strategy:
  1. Upgrade consumers to handle missing field
  2. Deploy new schema (v2)
  3. Upgrade producers to stop sending field
EOF
}

demonstrate_full_compatible() {
  log_scenario "Scenario 4: FULL Compatible Change"
  
  cat << 'EOF'
Change: Add optional field with default (safe both ways)

Schema Version 2:
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "country", "type": "string", "default": "US"}  ← NEW
  ]
}

Compatibility Test: FULL (BACKWARD + FORWARD)
────────────────────────────────────────────────

Scenario A: New producer → Old consumer
  Producer (v2) sends:    Consumer (v1) reads:
  {id, name, email,       {id, name, email}
   country: "CA"}         (ignores country)
  ✓ Works

Scenario B: Old producer → New consumer
  Producer (v1) sends:    Consumer (v2) reads:
  {id, name, email}       {id, name, email,
                           country: "US"}  ← default
  ✓ Works

Result: ✓ FULL COMPATIBLE

Best for: Rolling deployments, gradual migrations
EOF
}

demonstrate_breaking_change() {
  log_scenario "Scenario 5: BREAKING Change (Incompatible)"
  
  cat << 'EOF'
Breaking changes:

1. Rename field:
   v1: {"name": "Alice"}
   v2: {"full_name": "Alice"}
   ❌ Old consumers expect "name"

2. Change field type:
   v1: {"age": "25"}  (string)
   v2: {"age": 25}    (int)
   ❌ Type mismatch

3. Remove required field:
   v1: {"id", "name", "email"}
   v2: {"id", "name"}  (removed email)
   ❌ Old consumers expect "email"

4. Add required field without default:
   v1: {"id", "name"}
   v2: {"id", "name", "email"}  (required, no default)
   ❌ Old data doesn't have "email"

Result: ❌ INCOMPATIBLE

Solutions:
  1. Use field aliases for renames
  2. Create new topic for breaking changes
  3. Dual-write during migration period
  4. Coordinate full system upgrade
EOF
}

demonstrate_field_aliases() {
  log_scenario "Scenario 6: Safe Rename Using Aliases"
  
  cat << 'EOF'
Problem: Want to rename "name" → "full_name"

❌ Naive approach (breaks compatibility):
{
  "name": "full_name",  // Rename
  "type": "string"
}

✓ Correct approach (using aliases):
{
  "name": "full_name",
  "type": "string",
  "aliases": ["name"]    ← Allows reading old "name" field
}

How it works:
  Old data (v1):        New consumer (v2):
  {"name": "Alice"}  →  Reads "name" via alias
                        Maps to "full_name"
                        Result: full_name = "Alice"
                        ✓ Works!

  New data (v2):        Old consumer (v1):
  {"full_name": ...} →  Ignores unknown field
                        (If still have default/optional)
                        ✓ Works!

Best practice: Always use aliases for renames
EOF
}

demonstrate_evolution_timeline() {
  log_scenario "Scenario 7: Real-World Evolution Timeline"
  
  cat << 'EOF'
Evolution of User schema over 6 months:

┌─────────────────────────────────────────────────────────┐
│ MONTH 1: Version 1.0 (Initial)                          │
├─────────────────────────────────────────────────────────┤
│ Fields: id, name, email                                 │
│ Compatibility: N/A (initial)                            │
└─────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────┐
│ MONTH 2: Version 1.1 (Add optional phone)              │
├─────────────────────────────────────────────────────────┤
│ Fields: id, name, email, phone (optional)               │
│ Compatibility: BACKWARD ✓                               │
│ Reason: Old consumers ignore new field                  │
└─────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────┐
│ MONTH 3: Version 1.2 (Add country with default)        │
├─────────────────────────────────────────────────────────┤
│ Fields: id, name, email, phone, country (default: US)   │
│ Compatibility: FULL ✓                                   │
│ Reason: Old data gets default value                     │
└─────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────┐
│ MONTH 4: Version 1.3 (Add preferences)                 │
├─────────────────────────────────────────────────────────┤
│ Fields: ..., preferences (optional map)                 │
│ Compatibility: BACKWARD ✓                               │
└─────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────┐
│ MONTH 5: Version 2.0 (Major refactor)                  │
├─────────────────────────────────────────────────────────┤
│ Breaking changes: Restructure preferences               │
│ Solution: New topic "users-v2"                          │
│ Migration: Dual-write for 1 month                      │
└─────────────────────────────────────────────────────────┘

Lessons learned:
  • Small, frequent changes are safer
  • Always add fields as optional first
  • Use defaults for required fields
  • Major refactors need new topics
  • Test compatibility before deploying
EOF
}

demonstrate_compatibility_modes() {
  log_scenario "Scenario 8: Schema Registry Compatibility Modes"
  
  cat << 'EOF'
Schema Registry compatibility settings:

1. BACKWARD (Recommended for most use cases)
   ─────────────────────────────────────────
   • Consumers can read new and old data
   • Upgrade path: Consumers first, then producers
   • Safe changes: Add optional fields
   • Use when: Consumer-driven (read old data)

2. FORWARD (Less common)
   ─────────────────────────────────────────
   • Consumers can handle missing fields
   • Upgrade path: Producers first, then consumers
   • Safe changes: Remove optional fields
   • Use when: Producer-driven evolution

3. FULL (Most restrictive, safest)
   ─────────────────────────────────────────
   • Both backward and forward compatible
   • Can upgrade in any order
   • Safe changes: Add/remove optional fields with defaults
   • Use when: Large systems, complex deployments

4. NONE (Not recommended)
   ─────────────────────────────────────────
   • No compatibility checks
   • Any change allowed
   • Risk: Breaking changes slip through
   • Use when: Development only, never production

Setting compatibility:
  # Global setting
  curl -X PUT http://schema-registry:8081/config \
    -d '{"compatibility": "BACKWARD"}'
  
  # Per-topic setting
  curl -X PUT http://schema-registry:8081/config/user-events-value \
    -d '{"compatibility": "FULL"}'
EOF
}

demonstrate_best_practices() {
  log_section "Schema Evolution Best Practices"
  
  cat << 'EOF'
Best Practices Checklist:
─────────────────────────────────────────────────────

✓ Design for Evolution
  □ Always add fields as optional initially
  □ Provide sensible defaults for all fields
  □ Use unions for nullable types: ["null", "string"]
  □ Document schema changes in changelog

✓ Testing
  □ Test new schema against old consumers
  □ Test old schema against new consumers
  □ Use Schema Registry compatibility API
  □ Have rollback plan for failed migrations

✓ Deployment Strategy
  □ For BACKWARD: Upgrade consumers first
  □ For FORWARD: Upgrade producers first
  □ For FULL: Can upgrade in any order
  □ Monitor for serialization errors

✓ Documentation
  □ Document each schema version
  □ Explain breaking changes clearly
  □ Provide migration guides
  □ Track which services use which versions

✓ Monitoring
  □ Alert on compatibility check failures
  □ Monitor schema registration rate
  □ Track deserialization errors
  □ Log schema IDs in messages

✓ Naming Conventions
  □ Use semantic versioning (1.0.0, 1.1.0, 2.0.0)
  □ Clear field names (avoid abbreviations)
  □ Consistent naming style (camelCase or snake_case)
  □ Namespace your schemas (com.example.events)

✓ Advanced Techniques
  □ Use aliases for field renames
  □ Logical types for dates/decimals (Avro)
  □ Enums for constrained values
  □ Documentation in schema descriptions

❌ Avoid
  □ Removing required fields
  □ Changing field types
  □ Renaming fields without aliases
  □ Breaking changes in minor versions
  □ Hardcoding schema versions in code
EOF
}

main() {
  log_section "Schema Evolution Demonstration"
  
  log_info "This demo shows schema evolution concepts"
  log_info "For actual Avro/Protobuf testing, set up Schema Registry"
  log_info "(See infra/docker-compose-full.yml)"
  
  # Run scenarios
  demonstrate_schema_v1
  demonstrate_backward_compatible
  demonstrate_forward_compatible
  demonstrate_full_compatible
  demonstrate_breaking_change
  demonstrate_field_aliases
  demonstrate_evolution_timeline
  demonstrate_compatibility_modes
  demonstrate_best_practices
  
  log_section "Demonstration Complete"
  
  echo ""
  echo "Key Takeaways:"
  echo "  1. Plan for evolution from day one"
  echo "  2. Use BACKWARD compatibility for most cases"
  echo "  3. Always add fields as optional initially"
  echo "  4. Test compatibility before deploying"
  echo "  5. Breaking changes need careful coordination"
  echo ""
  echo "Next Steps:"
  echo "  • Set up Schema Registry (see docker-compose-full.yml)"
  echo "  • Practice with Avro schemas"
  echo "  • Configure compatibility mode for your topics"
  echo "  • Implement schema validation in producers"
  echo ""
}

main "$@"
