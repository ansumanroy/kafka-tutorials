# Java Examples for Kafka Tutorials

This directory contains Java implementations of the Kafka tutorials using the official Kafka Java client with JUnit 5 integration tests.

## 📚 Available Chapters

### Advanced Chapter 01: Reliability & Delivery Guarantees ✅
**Location:** `adv_chapter_01_reliability/`

**Topics Covered:**
- Different `acks` configurations (0, 1, all)
- Idempotence and exactly-once semantics
- Retry behavior and timeout handling
- Failure simulation and recovery

**Quick Start:**
```bash
make java-reliability-build
make java-reliability-test
```

**See:** [adv_chapter_01_reliability/README.md](adv_chapter_01_reliability/README.md)

---

### Advanced Chapter 02: Performance & Throughput ✅
**Location:** `adv_chapter_02_performance/`

**Topics Covered:**
- High throughput vs low latency configurations
- Compression algorithm comparison (none, gzip, snappy, lz4, zstd)
- Batch size and linger time optimization
- Performance benchmarking and metrics

**Quick Start:**
```bash
make java-performance-build
make java-performance-benchmark    # Run standalone benchmark
make java-performance-test         # Run all tests
```

**See:** [adv_chapter_02_performance/README.md](adv_chapter_02_performance/README.md)

---

### Advanced Chapter 03: Keys, Partitioning, and Ordering ✅
**Location:** `adv_chapter_03_partitioning/`

**Topics Covered:**
- Message keys and partition routing
- Ordering guarantees (within partition vs cross-partition)
- Hot partition problem (celebrity effect)
- Composite key solution for load distribution
- Partition distribution analysis

**Quick Start:**
```bash
make java-partitioning-build
make java-partitioning-demo        # Run demonstration
make java-partitioning-test        # Run all tests
```

**See:** [adv_chapter_03_partitioning/README.md](adv_chapter_03_partitioning/README.md)

---

## 🚀 Quick Start

### Prerequisites

- Java 17 or higher
- Gradle (wrapper included)
- Running Kafka cluster

### Setup Kafka

```bash
# Start Kafka using Docker
make kafka-apache-start

# Create environment file
cp infra/env-example.local infra/env.local
source infra/env.local
```

### Build and Run

```bash
# Build all chapters
make java-reliability-build
make java-performance-build

# Run tests
make java-reliability-test
make java-performance-test

# Run benchmarks
make java-performance-benchmark
```

## 📊 Available Make Targets

### Chapter 01: Reliability

```bash
make java-reliability-build         # Build
make java-reliability-test          # Run all tests
make java-reliability-integration   # Integration tests
make java-reliability-failure       # Failure simulation
make java-reliability-clean         # Clean artifacts
```

### Chapter 02: Performance

```bash
make java-performance-build          # Build
make java-performance-test           # Run all tests
make java-performance-benchmark      # Standalone benchmark
make java-performance-tests-only     # Perf tests only
make java-performance-compression    # Compression tests
make java-performance-clean          # Clean artifacts
```

### Chapter 03: Partitioning

```bash
make java-partitioning-build         # Build
make java-partitioning-test          # Run all tests
make java-partitioning-demo          # Run demonstration
make java-partitioning-behavior      # Behavior tests
make java-partitioning-hotpartition  # Hot partition tests
make java-partitioning-clean         # Clean artifacts
```

## 📁 Project Structure

```
java/
├── README.md
├── adv_chapter_01_reliability/     # Reliability & delivery guarantees
│   ├── build.gradle
│   ├── settings.gradle
│   ├── README.md
│   └── src/
│       ├── main/java/.../reliability/
│       │   └── ProducerConfigHelper.java
│       └── test/java/.../reliability/
│           ├── ReliabilityIntegrationTest.java
│           └── FailureSimulationTest.java
│
└── adv_chapter_02_performance/     # Performance & throughput
    ├── build.gradle
    ├── settings.gradle
    ├── README.md
    └── src/
        ├── main/java/.../performance/
        │   ├── PerformanceConfigHelper.java
        │   ├── BenchmarkResult.java
        │   └── ThroughputBenchmark.java
        └── test/java/.../performance/
            ├── PerformanceBenchmarkTest.java
            └── CompressionComparisonTest.java
```

## 🧪 Running Tests

### From Root Directory (Recommended)

```bash
# Run all tests for a chapter
make java-reliability-test
make java-performance-test

# Run specific test suites
make java-reliability-integration
make java-performance-compression
```

### From Chapter Directory

```bash
# Navigate to chapter
cd adv_chapter_02_performance

# Run all tests
./gradlew test

# Run specific test
./gradlew test --tests CompressionComparisonTest

# Run tagged tests
./gradlew runPerformanceTests
./gradlew runCompressionTests

# Run benchmark
./gradlew runBenchmark
```

## 📈 Example Output

### Reliability Tests
```
ReliabilityIntegrationTest > testAcksOne() PASSED
ReliabilityIntegrationTest > testAcksAll() PASSED
ReliabilityIntegrationTest > testIdempotence() PASSED

✓ 12 tests passed
```

### Performance Benchmark
```
╔═══════════════════════════════════════════════════════════╗
║  Performance Comparison Summary                           ║
╚═══════════════════════════════════════════════════════════╝

Configuration          |   Throughput    |  Bandwidth   | P95 Latency
─────────────────────────────────────────────────────────────────────
compression-none       |  8,234.12 msg/s |  8.04 MB/s  |    42 ms
compression-lz4        |  9,560.23 msg/s |  9.34 MB/s  |    35 ms
compression-snappy     |  8,891.23 msg/s |  8.68 MB/s  |    38 ms
compression-gzip       |  6,123.45 msg/s |  5.98 MB/s  |    67 ms

🏆 Best Throughput: compression-lz4 (9,560.23 msg/sec)
⚡ Best Latency:    compression-lz4 (35 ms p95)
```

## 🎯 Learning Path

1. **Chapter 01: Reliability**
   - Understand producer reliability settings
   - Test different `acks` configurations
   - Learn about idempotence and retries
   - Simulate and handle failures

2. **Chapter 02: Performance**
   - Benchmark producer throughput
   - Compare compression algorithms
   - Optimize batch size and linger time
   - Understand throughput vs latency tradeoffs

3. **Apply to Production**
   - Use configuration helpers in your code
   - Adapt patterns to your requirements
   - Monitor and tune based on benchmarks

## 🔧 Environment Configuration

### Local Docker (Recommended)

```bash
# Start Kafka
make kafka-apache-start

# Setup environment
cp infra/env-example.local infra/env.local
source infra/env.local

# Verify
echo $KAFKA_BOOTSTRAP_SERVERS  # localhost:9092
```

### AWS MSK

```bash
# Create environment file
cp infra/env-example.msk infra/env.msk

# Edit with MSK details
vim infra/env.msk

# Source it
source infra/env.msk
```

## 🔍 Troubleshooting

### Build Issues

```bash
# Clean and rebuild
cd adv_chapter_XX_xxx
./gradlew clean build

# Or from root
make java-xxx-clean
make java-xxx-build
```

### Connection Failures

```bash
# Check Kafka status
make kafka-apache-status

# Verify environment
source infra/env.local
echo $KAFKA_BOOTSTRAP_SERVERS

# Test connection
make test-connection
```

### OutOfMemoryError

```bash
# Increase Gradle heap
export GRADLE_OPTS="-Xmx2g"
./gradlew test
```

### Test Timeouts

```bash
# Tests have 10-minute timeout by default
# If needed, edit build.gradle:
systemProperty 'junit.jupiter.execution.timeout.default', '15m'
```

## 📚 Additional Resources

- **Bash Equivalents:** `bash/chapters/adv_chapter_XX_xxx/`
- **Quick Start Guide:** `QUICKSTART.md`
- **Docker Setup:** `infra/README-DOCKER.md`
- **Kafka Documentation:** https://kafka.apache.org/documentation/

## 🎓 Planned Chapters

Future chapters will include:
- Chapter 04: Serialization & Schema Management
- Chapter 05: Error Handling & Observability
- Chapter 06: Operational Concerns
- Basic chapters (connectivity, topics, consumers, etc.)

## 💡 Using in Your Projects

### Example: Using Configuration Helpers

```java
import com.kafkatutorials.reliability.ProducerConfigHelper;
import com.kafkatutorials.performance.PerformanceConfigHelper;

// Reliable producer
Properties reliableConfig = ProducerConfigHelper.getReliableConfig();

// High throughput producer
Properties highThroughputConfig = PerformanceConfigHelper.getHighThroughputConfig();

// Create producer
KafkaProducer<String, String> producer = new KafkaProducer<>(config);
```

### Example: Running Benchmarks

```java
import com.kafkatutorials.performance.ThroughputBenchmark;

ThroughputBenchmark benchmark = new ThroughputBenchmark();

// Run compression comparison
List<BenchmarkResult> results = benchmark.compareCompression(
    "my-topic", 10000, 1024
);

ThroughputBenchmark.printComparisonTable(results);
```

## 🤝 Contributing

When adding new chapters:
1. Follow the existing structure
2. Include comprehensive tests
3. Add Makefile targets
4. Document in chapter README
5. Update this main README

---

**Quick Reference:**
- 🏗️  Build: `make java-xxx-build`
- 🧪 Test: `make java-xxx-test`
- 🧹 Clean: `make java-xxx-clean`
- 📖 Help: `make help`
