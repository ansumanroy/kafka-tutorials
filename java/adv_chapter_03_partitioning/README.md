# Advanced Chapter 03 - Keys, Partitioning, and Ordering (Java)

This Java project demonstrates Kafka partitioning behavior, key-based routing, ordering guarantees, and solutions to hot partition problems.

## 📋 Overview

This chapter covers:
- **Message Keys & Partitioning**: How keys determine partition routing
- **Ordering Guarantees**: Within-partition vs cross-partition ordering
- **Hot Partition Problem**: When one key gets disproportionate traffic
- **Composite Keys Solution**: Distributing hot key load across partitions
- **Distribution Analysis**: Tools to analyze partition balance

## 🏗️ Project Structure

```
adv_chapter_03_partitioning/
├── build.gradle
├── settings.gradle
├── README.md
└── src/
    ├── main/java/com/kafkatutorials/partitioning/
    │   ├── CompositeKeyPartitioner.java          # Custom partitioner
    │   ├── PartitionInfo.java                    # Partition metadata
    │   ├── PartitionDistributionAnalyzer.java    # Distribution analyzer
    │   ├── PartitioningHelper.java               # Config helpers
    │   └── PartitioningDemo.java                 # Demo application
    │
    └── test/java/com/kafkatutorials/partitioning/
        ├── PartitioningBehaviorTest.java         # Basic partitioning tests
        └── HotPartitionTest.java                 # Hot partition tests
```

## 🚀 Quick Start

### Prerequisites

1. Kafka cluster running:
```bash
make kafka-apache-start
source infra/env.local
```

2. Java 17+ and Gradle

### Run the Demo

```bash
cd java/adv_chapter_03_partitioning

# Run partitioning demonstration
gradle runPartitioningDemo

# Or from root
make java-partitioning-demo
```

### Run Tests

```bash
# All tests
gradle test

# Partitioning behavior tests only
gradle runPartitioningTests

# Hot partition tests only
gradle runHotPartitionTests

# From root directory
make java-partitioning-test
```

## 📊 What Gets Demonstrated

### 1. Same Key → Same Partition

```java
// All messages with key "user-123" go to the same partition
producer.send(new ProducerRecord<>(topic, "user-123", "message-1"));
producer.send(new ProducerRecord<>(topic, "user-123", "message-2"));
producer.send(new ProducerRecord<>(topic, "user-123", "message-3"));
// All go to partition X (determined by hash)
```

### 2. Different Keys → Distributed

```java
// Different keys are distributed across partitions
producer.send(new ProducerRecord<>(topic, "user-A", "data"));  // → Partition 0
producer.send(new ProducerRecord<>(topic, "user-B", "data"));  // → Partition 2
producer.send(new ProducerRecord<>(topic, "user-C", "data"));  // → Partition 1
```

### 3. Null Keys → Round-Robin

```java
// Null keys use round-robin distribution
producer.send(new ProducerRecord<>(topic, null, "log-1"));  // → Partition 0
producer.send(new ProducerRecord<>(topic, null, "log-2"));  // → Partition 1
producer.send(new ProducerRecord<>(topic, null, "log-3"));  // → Partition 2
```

### 4. Ordering Within Partition

```java
// Messages with same key are ordered within partition
String key = "order-test";
producer.send(new ProducerRecord<>(topic, key, "step-1"));  // Offset 100
producer.send(new ProducerRecord<>(topic, key, "step-2"));  // Offset 101
producer.send(new ProducerRecord<>(topic, key, "step-3"));  // Offset 102
// Sequential offsets guarantee order
```

## 🔥 Hot Partition Problem & Solution

### Problem: Celebrity Effect

```java
// One key gets 70% of traffic - creates hot partition
String celebrity = "celebrity-user-1234";

for (int i = 0; i < 70; i++) {
    producer.send(new ProducerRecord<>(topic, celebrity, "msg-" + i));
}
// All 70 messages go to ONE partition → hot spot!
```

**Result:**
- One partition overloaded
- Uneven consumer load
- Potential performance bottleneck

### Solution: Composite Keys

```java
// Add suffix to distribute load
String celebrityId = "celebrity-user-1234";

for (int i = 0; i < 70; i++) {
    String compositeKey = PartitioningHelper.generateCompositeKeyWithSequence(
        celebrityId, i
    );
    // compositeKey = "celebrity-user-1234-00000042"
    producer.send(new ProducerRecord<>(topic, compositeKey, "msg-" + i));
}
// Messages distributed across multiple partitions!
```

**Result:**
- Load distributed evenly
- Better consumer parallelism
- Improved throughput

## 📈 Sample Output

### Partitioning Demo

```
╔═══════════════════════════════════════════════════════════╗
║  Demo 1: Same Key → Same Partition                       ║
╚═══════════════════════════════════════════════════════════╝

Sending 10 messages with key: user-123
  Message 0 → Partition 1 (offset 0)
  Message 1 → Partition 1 (offset 1)
  Message 2 → Partition 1 (offset 2)
  ...
  Message 9 → Partition 1 (offset 9)

✓ Result: All messages went to 1 partition(s): [1]
  Expected: 1 partition (same key → same partition)
```

### Distribution Analysis

```
╔═══════════════════════════════════════════════════════════╗
║  Partition Distribution Analysis                          ║
╚═══════════════════════════════════════════════════════════╝

📊 Topic: partitioning-demo-topic
Total Messages: 120
Partition Count: 6
Average per Partition: 20
Standard Deviation: 5.24

📈 Distribution by Partition:
Partition  | Messages     | Percentage | Distribution
───────────────────────────────────────────────────────────────────────
0          |           18 |     15.00% | ███████
1          |           25 |     20.83% | ██████████
2          |           19 |     15.83% | ████████
3          |           22 |     18.33% | █████████
4          |           20 |     16.67% | ████████
5          |           16 |     13.33% | ███████

🔥 Hot/Cold Analysis:
  • Hottest partition: 1 (25 messages, 20.8%)
  • Coldest partition: 5 (16 messages, 13.3%)
  • Hottest/Coldest ratio: 1.56x
```

### Hot Partition Problem

```
╔═══════════════════════════════════════════════════════════╗
║  Hot Partition Problem Demonstration                      ║
╚═══════════════════════════════════════════════════════════╝

Message Distribution:
  celebrity-user-1234      :  70 messages (70.0%)
  user-A                   :   6 messages (6.0%)
  user-B                   :   6 messages (6.0%)
  user-C                   :   6 messages (6.0%)
  user-D                   :   6 messages (6.0%)
  user-E                   :   6 messages (6.0%)

⚠️  WARNING: Hot partition detected!
   Some partitions have >2x average load.
   Consider using composite keys or custom partitioner.
```

## 🔧 Using the Classes

### PartitioningHelper

```java
import com.kafkatutorials.partitioning.PartitioningHelper;

// Get configuration with default partitioner
Properties props = PartitioningHelper.getDefaultPartitionerConfig();

// Get configuration with composite key partitioner
Properties props = PartitioningHelper.getCompositeKeyPartitionerConfig();

// Generate composite keys
String key1 = PartitioningHelper.generateCompositeKey("user-123", "suffix-A");
String key2 = PartitioningHelper.generateCompositeKeyWithTimestamp("user-123");
String key3 = PartitioningHelper.generateCompositeKeyWithSequence("user-123", 42);
```

### PartitionDistributionAnalyzer

```java
import com.kafkatutorials.partitioning.PartitionDistributionAnalyzer;

String bootstrapServers = "localhost:9092";
PartitionDistributionAnalyzer analyzer = 
    new PartitionDistributionAnalyzer(bootstrapServers);

// Analyze distribution
PartitionDistributionAnalyzer.DistributionReport report = 
    analyzer.analyzeDistribution("my-topic");

// Print detailed report
report.print();

// Get metrics
long totalMessages = report.getTotalMessages();
long avgPerPartition = report.getAverageMessagesPerPartition();
double stdDev = report.getStandardDeviation();
boolean hasHotSpots = report.hasHotPartitions(2.0); // >2x average
```

### Custom Partitioner

```java
import com.kafkatutorials.partitioning.CompositeKeyPartitioner;

Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, 
         CompositeKeyPartitioner.class.getName());

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
```

## 🧪 Running Specific Tests

```bash
# All partitioning tests
./gradlew test --tests PartitioningBehaviorTest

# Specific test
./gradlew test --tests PartitioningBehaviorTest.testSameKeyToSamePartition
./gradlew test --tests HotPartitionTest.testHotPartitionProblem

# Tagged tests
./gradlew runPartitioningTests     # @Tag("partitioning")
./gradlew runHotPartitionTests     # @Tag("hot-partition")
```

## 📝 Key Concepts

### 1. Default Partitioning

Kafka's default partitioner uses:
```
partition = hash(key) % partition_count
```

- Same key always → same partition
- Different keys → distributed
- Null key → round-robin

### 2. Ordering Guarantees

**Within Partition:** ✅ Guaranteed
- Messages with same key
- Sequential offsets
- FIFO order

**Across Partitions:** ❌ Not Guaranteed
- Different partitions are independent
- No global ordering
- Parallel processing

### 3. Hot Partition Solutions

| Approach | Pros | Cons | Use Case |
|----------|------|------|----------|
| **Composite Keys** | Simple, maintains grouping | Requires key design | Celebrity users, hot products |
| **Custom Partitioner** | Full control | More complex | Special routing logic |
| **More Partitions** | More parallelism | More overhead | General scalability |
| **Different Topic** | Isolates hot data | Data duplication | Separate hot/cold paths |

## 🎯 Production Recommendations

### Key Selection Strategy

```java
// Good: Natural business keys
String key = userId;              // User events
String key = orderId;             // Order processing
String key = sessionId;           // Session data

// Bad: Random or sequential
String key = UUID.randomUUID();   // Defeats ordering
String key = String.valueOf(i++); // Sequential collision

// Solution for hot keys: Composite
String key = userId + "-" + shardId;  // Distribute load
```

### Partition Count Planning

```java
// Formula: partitions >= max(consumers, throughput_requirement / per_partition_throughput)

// Example:
// - Want 10 concurrent consumers
// - Need 100 MB/sec throughput
// - Each partition handles 10 MB/sec
// Partitions needed: max(10, 100/10) = 10 partitions
```

### Monitoring Hot Partitions

```java
// Regular analysis
PartitionDistributionAnalyzer analyzer = new PartitionDistributionAnalyzer(servers);
DistributionReport report = analyzer.analyzeDistribution("my-topic");

// Alert if hot partitions detected
if (report.hasHotPartitions(2.0)) {
    logger.warn("Hot partition detected in topic: {}", topicName);
    // Take action: composite keys, rebalance, scale up
}

// Track standard deviation
double stdDev = report.getStandardDeviation();
double mean = report.getAverageMessagesPerPartition();
double cv = stdDev / mean;  // Coefficient of variation

if (cv > 0.3) {
    logger.warn("High partition skew detected: CV = {}", cv);
}
```

## 🔍 Troubleshooting

### Uneven Distribution

**Symptom:** Some partitions have much more data

**Causes:**
- Hot keys (celebrity effect)
- Poor key selection (sequential IDs)
- Hash collisions

**Solutions:**
- Use composite keys
- Custom partitioner
- Increase partition count
- Review key design

### Ordering Issues

**Symptom:** Messages out of order

**Causes:**
- Different partitions
- Retries with max.in.flight > 1
- Consumer processing order

**Solutions:**
- Ensure same key for related messages
- Use `max.in.flight.requests.per.connection=1` for strict ordering
- Enable idempotence

### Performance Bottlenecks

**Symptom:** One consumer lagging

**Causes:**
- Hot partition
- Uneven load distribution
- Consumer group imbalance

**Solutions:**
- Composite keys to distribute hot data
- More partitions (and consumers)
- Partition reassignment

## 📚 Related Documentation

- **Bash equivalent**: `bash/chapters/adv_chapter_03_partitioning/`
- **Chapter 02 (Performance)**: `java/adv_chapter_02_performance/`
- **Kafka Partitioning Docs**: https://kafka.apache.org/documentation/#design_partitioning

## 🎓 Learning Path

1. **Run Demo**: `gradle runPartitioningDemo` - See basic behavior
2. **Run Tests**: `gradle test` - Understand guarantees
3. **Analyze Distribution**: Use `PartitionDistributionAnalyzer`
4. **Simulate Hot Partition**: Run `HotPartitionTest`
5. **Apply Composite Keys**: Test solution approaches

## 📊 Gradle Tasks

| Task | Description |
|------|-------------|
| `./gradlew build` | Build project |
| `./gradlew test` | Run all tests |
| `./gradlew runPartitioningDemo` | Run demo application |
| `./gradlew runPartitioningTests` | Run partitioning tests |
| `./gradlew runHotPartitionTests` | Run hot partition tests |

## 🎯 Key Takeaways

1. **Same key → Same partition** (always)
2. **Ordering guaranteed within partition only**
3. **Hot partitions are a real production problem**
4. **Composite keys effectively distribute hot key load**
5. **Monitor partition distribution regularly**
6. **Choose keys based on your ordering needs**

---

**Next Steps:**
- Move to Chapter 04: Serialization & Schema Management
- Apply partitioning strategies to your workload
- Monitor and tune partition distribution

Happy partitioning! 🎯
