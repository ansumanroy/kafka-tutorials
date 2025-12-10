# Advanced Chapter 04 - Serialization & Schema Management (Java)

This Java project demonstrates Kafka message serialization strategies, Schema Registry integration (Redpanda/Confluent compatible), and schema evolution patterns.

## 📋 Overview

This chapter covers:
- **Serialization Formats**: String, JSON, Avro comparison
- **Schema Registry**: Registration, retrieval, versioning
- **Avro Schemas**: Define, generate, use strongly-typed classes
- **Schema Evolution**: Backward, forward, and full compatibility
- **Size Comparison**: Binary vs text format efficiency
- **Production Patterns**: Best practices for schema management

## 🏗️ Project Structure

```
adv_chapter_04_serialization/
├── build.gradle                    # With Avro plugin
├── settings.gradle
├── README.md
└── src/
    ├── main/
    │   ├── avro/                   # Avro schema definitions (.avsc)
    │   │   ├── user_v1.avsc        # Version 1: Base schema
    │   │   ├── user_v2.avsc        # Version 2: Backward compatible
    │   │   ├── user_v3.avsc        # Version 3: Forward compatible
    │   │   └── order.avsc          # Complex nested schema
    │   │
    │   └── java/com/kafkatutorials/serialization/
    │       ├── UserEvent.java               # POJO for JSON
    │       ├── SerializationHelper.java     # Config factory
    │       └── SerializationDemo.java       # Demo application
    │
    └── test/java/com/kafkatutorials/serialization/
        ├── FormatComparisonTest.java        # Format tests
        ├── SchemaEvolutionTest.java         # Evolution tests
        └── AvroSerializationTest.java       # Avro tests
```

## 🚀 Quick Start

### Prerequisites

1. **Kafka with Schema Registry** running:
```bash
# Option 1: Use full stack (includes Schema Registry)
make kafka-start  # Confluent Platform with Schema Registry

# Option 2: Redpanda with Schema Registry
docker run -d --name redpanda \
  -p 9092:9092 -p 8081:8081 \
  docker.redpanda.com/redpandadata/redpanda:latest \
  redpanda start --schema-registry-enabled
```

2. **Set environment variables**:
```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SCHEMA_REGISTRY_URL="http://localhost:8081"
```

### Build Project

```bash
cd java/adv_chapter_04_serialization

# Generate Avro classes from schemas
gradle build

# This creates Java classes from .avsc files:
# - UserV1, UserV2, UserV3
# - Order, OrderItem
```

### Run Demo

```bash
gradle runSerializationDemo

# Or from root
make java-serialization-demo
```

### Run Tests

```bash
gradle test

# Or specific test suites
gradle runFormatTests          # Format comparison
gradle runSchemaTests          # Schema evolution
gradle runAvroTests            # Avro serialization
```

## 📊 Serialization Formats

### 1. String Serialization (Simple)

```java
Properties props = SerializationHelper.getStringProducerConfig();
KafkaProducer<String, String> producer = new KafkaProducer<>(props);

// Send simple string
producer.send(new ProducerRecord<>(topic, "key", "Hello, Kafka!"));

// Size: ~13 bytes
// Pros: Simple, human-readable
// Cons: No structure, no validation
```

### 2. JSON Serialization (Structured)

```java
Properties props = SerializationHelper.getJsonProducerConfig();
KafkaProducer<String, UserEvent> producer = new KafkaProducer<>(props);

UserEvent user = new UserEvent("123", "john", "john@example.com", System.currentTimeMillis());
producer.send(new ProducerRecord<>(topic, user.getUserId(), user));

// Size: ~80-100 bytes
// Pros: Flexible, readable, language-agnostic
// Cons: Verbose, no schema enforcement, larger size
```

### 3. Avro Serialization (Binary + Schema)

```java
Properties props = SerializationHelper.getAvroProducerConfig();
KafkaProducer<String, UserV1> producer = new KafkaProducer<>(props);

UserV1 user = UserV1.newBuilder()
    .setUserId("123")
    .setUsername("john")
    .setEmail("john@example.com")
    .setTimestamp(System.currentTimeMillis())
    .build();

producer.send(new ProducerRecord<>(topic, user.getUserId(), user));

// Size: ~35-40 bytes
// Pros: Compact, schema evolution, validation, strongly-typed
// Cons: Not human-readable, requires Schema Registry
```

## 🔄 Schema Evolution

### Version 1: Base Schema

```json
{
  "type": "record",
  "name": "UserV1",
  "fields": [
    {"name": "userId", "type": "string"},
    {"name": "username", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}
```

### Version 2: Backward Compatible (Add Optional Field)

```json
{
  "type": "record",
  "name": "UserV2",
  "fields": [
    {"name": "userId", "type": "string"},
    {"name": "username", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "phoneNumber", "type": ["null", "string"], "default": null}
  ]
}
```

**Backward Compatible**: Old consumers can read new data (ignore new field)

### Version 3: Forward Compatible (Remove Field with Default)

```json
{
  "type": "record",
  "name": "UserV3",
  "fields": [
    {"name": "userId", "type": "string"},
    {"name": "username", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "status", "type": "enum", "default": "ACTIVE"}
  ]
}
```

**Forward Compatible**: New consumers can read old data (use defaults)

## 📈 Schema Registry Workflow

### Producer Side

```java
// 1. Create Avro record
UserV1 user = UserV1.newBuilder()
    .setUserId("123")
    .setUsername("john")
    .setEmail("john@example.com")
    .setTimestamp(System.currentTimeMillis())
    .build();

// 2. Serializer automatically:
//    - Checks Schema Registry for existing schema
//    - Registers if new
//    - Gets schema ID
//    - Encodes: [magic byte][schema ID][avro binary data]

// 3. Send to Kafka
producer.send(new ProducerRecord<>(topic, key, user));
```

### Consumer Side

```java
// 1. Read from Kafka
ConsumerRecords<String, UserV1> records = consumer.poll(Duration.ofMillis(100));

for (ConsumerRecord<String, UserV1> record : records) {
    // 2. Deserializer automatically:
    //    - Extracts schema ID from message
    //    - Fetches schema from Registry
    //    - Deserializes using schema
    //    - Returns strongly-typed object
    
    UserV1 user = record.value();
    System.out.println("User: " + user.getUsername());
}
```

## 🔧 Using the Helper Classes

### SerializationHelper

```java
import com.kafkatutorials.serialization.SerializationHelper;

// Get producer configs for different formats
Properties stringConfig = SerializationHelper.getStringProducerConfig();
Properties jsonConfig = SerializationHelper.getJsonProducerConfig();
Properties avroConfig = SerializationHelper.getAvroProducerConfig();

// Get consumer configs
Properties stringConsumerConfig = SerializationHelper.getStringConsumerConfig();
Properties jsonConsumerConfig = SerializationHelper.getJsonConsumerConfig();
Properties avroConsumerConfig = SerializationHelper.getAvroConsumerConfig();

// Print config
SerializationHelper.printConfig(avroConfig, "Avro");
```

### UserEvent (JSON POJO)

```java
import com.kafkatutorials.serialization.UserEvent;

// Create user event
UserEvent user = new UserEvent(
    "user-123",
    "john_doe",
    "john@example.com",
    System.currentTimeMillis(),
    "+1-555-0123"  // Optional phone
);

// Serialize to JSON automatically by KafkaJsonSerializer
producer.send(new ProducerRecord<>(topic, user.getUserId(), user));
```

### Avro Generated Classes

```java
import com.kafkatutorials.serialization.avro.UserV1;
import com.kafkatutorials.serialization.avro.UserV2;
import com.kafkatutorials.serialization.avro.Order;
import com.kafkatutorials.serialization.avro.OrderItem;

// Use builder pattern (generated by Avro)
UserV1 user = UserV1.newBuilder()
    .setUserId("123")
    .setUsername("john")
    .setEmail("john@example.com")
    .setTimestamp(System.currentTimeMillis())
    .build();

// Complex nested structures
Order order = Order.newBuilder()
    .setOrderId("order-456")
    .setUserId("user-123")
    .setItems(Arrays.asList(
        OrderItem.newBuilder()
            .setProductId("prod-1")
            .setQuantity(2)
            .setPrice(19.99)
            .build()
    ))
    .setTotalAmount(39.98)
    .setTimestamp(System.currentTimeMillis())
    .build();
```

## 🧪 Running Specific Tests

```bash
# All tests
./gradlew test

# Format comparison tests
./gradlew test --tests FormatComparisonTest

# Schema evolution tests
./gradlew test --tests SchemaEvolutionTest

# Avro serialization tests
./gradlew test --tests AvroSerializationTest

# Tagged tests
./gradlew runFormatTests     # @Tag("formats")
./gradlew runSchemaTests     # @Tag("schema")
./gradlew runAvroTests       # @Tag("avro")
```

## 📝 Compatibility Modes

### Backward Compatibility

**Rule**: Old consumers can read new data

**How**: Add optional fields (with defaults or nullable)

```java
// V1 → V2: Added optional phoneNumber
UserV2 userV2 = UserV2.newBuilder()
    .setUserId("123")
    .setUsername("john")
    .setEmail("john@example.com")
    .setTimestamp(System.currentTimeMillis())
    .setPhoneNumber("+1-555-0123")  // NEW OPTIONAL FIELD
    .build();

// Old V1 consumer can still read this (ignores phoneNumber)
```

### Forward Compatibility

**Rule**: New consumers can read old data

**How**: Remove fields or add fields with defaults

```java
// V1 data (missing 'status' field)
// V3 consumer reads it and uses default: ACTIVE
```

### Full Compatibility

**Rule**: Both directions work

**How**: Only add/remove optional fields with defaults

## 🎯 Production Recommendations

### Schema Design Best Practices

```java
// ✅ GOOD: Use optional fields for extensibility
{"name": "phoneNumber", "type": ["null", "string"], "default": null}

// ✅ GOOD: Provide defaults for new fields
{"name": "status", "type": "string", "default": "ACTIVE"}

// ✅ GOOD: Use enums for fixed sets
{"name": "status", "type": "enum", "symbols": ["ACTIVE", "INACTIVE"]}

// ❌ BAD: Required fields break backward compatibility
{"name": "newField", "type": "string"}  // No default!

// ❌ BAD: Removing required fields breaks forward compatibility
// Don't remove fields without planning
```

### Schema Registry Configuration

```java
// Enable auto-registration (development)
props.put("auto.register.schemas", "true");

// Disable auto-registration (production - register via CI/CD)
props.put("auto.register.schemas", "false");

// Use specific Avro reader for strongly-typed objects
props.put("specific.avro.reader", "true");
```

### Format Selection Guide

| Use Case | Format | Why |
|----------|--------|-----|
| **Logs, simple events** | String | Simple, readable |
| **APIs, debugging** | JSON | Flexible, debuggable |
| **High throughput** | Avro | Compact, fast |
| **Long-term storage** | Avro | Schema evolution |
| **Cross-language** | Avro/Protobuf | Language-agnostic |
| **Strict contracts** | Avro | Schema validation |

## 🔍 Troubleshooting

### Schema Registry Connection Issues

```bash
# Test connection
curl http://localhost:8081/subjects

# Check if running
docker ps | grep schema-registry

# Check logs
docker logs <schema-registry-container>
```

### Schema Not Found

```java
// Error: Schema not found in registry

// Solution 1: Enable auto-registration
props.put("auto.register.schemas", "true");

// Solution 2: Register schema manually
// Use Schema Registry REST API or Confluent CLI
```

### Incompatible Schema Changes

```java
// Error: Schema is incompatible

// Check compatibility:
// GET /compatibility/subjects/{subject}/versions/latest

// Common causes:
// - Removed required field
// - Changed field type
// - Removed enum value

// Solutions:
// - Add fields as optional (with default)
// - Use schema versioning correctly
// - Review compatibility mode settings
```

### Avro Classes Not Generated

```bash
# Make sure Avro plugin is applied
gradle clean build

# Check build/generated-main-avro-java/ for generated classes

# If missing, verify:
# 1. Avro schemas in src/main/avro/
# 2. Valid JSON schema format
# 3. Avro plugin in build.gradle
```

## 📚 Related Documentation

- **Bash equivalent**: `bash/chapters/adv_chapter_04_serialization/`
- **Apache Avro Docs**: https://avro.apache.org/docs/current/
- **Schema Registry Docs**: https://docs.confluent.io/platform/current/schema-registry/
- **Redpanda Schema Registry**: https://docs.redpanda.com/docs/manage/schema-reg/

## 🎓 Learning Path

1. **Start Simple**: Use String serialization
2. **Add Structure**: Move to JSON for flexibility
3. **Add Validation**: Introduce Avro with schemas
4. **Learn Evolution**: Test backward/forward compatibility
5. **Production Ready**: Implement schema governance

## 📊 Gradle Tasks

| Task | Description |
|------|-------------|
| `./gradlew build` | Build + generate Avro classes |
| `./gradlew test` | Run all tests |
| `./gradlew runSerializationDemo` | Run demo |
| `./gradlew runFormatTests` | Format comparison tests |
| `./gradlew runSchemaTests` | Schema evolution tests |
| `./gradlew runAvroTests` | Avro serialization tests |

## 🎯 Key Takeaways

1. **String**: Simple but no structure
2. **JSON**: Flexible but verbose  
3. **Avro**: Compact + schema evolution = production choice
4. **Schema Registry**: Central schema management
5. **Compatibility**: Plan schema changes carefully
6. **Backward**: Add optional fields
7. **Forward**: Remove fields or use defaults
8. **Full**: Only optional changes

---

**Next Steps:**
- Move to Chapter 05: Error Handling & Observability
- Apply serialization patterns to your data
- Set up Schema Registry for production

Happy serializing! 📦
