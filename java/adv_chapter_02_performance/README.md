# Advanced Chapter 02 - Performance & Throughput (Java)

This Java project provides comprehensive performance benchmarking tools for Kafka producers, demonstrating how different configurations affect throughput, latency, and resource utilization.

## 📋 Overview

This chapter covers:
- **Performance Profiles**: High throughput, low latency, and balanced configurations
- **Compression Comparison**: Testing all Kafka compression algorithms (none, gzip, snappy, lz4, zstd)
- **Batch Size Tuning**: Impact of different batch sizes on throughput
- **Linger Time Effects**: How linger.ms affects batching and latency
- **Message Size Impact**: Performance characteristics with different message sizes

## 🏗️ Project Structure

```
adv_chapter_02_performance/
├── build.gradle                  # Gradle build configuration
├── settings.gradle               # Project settings
└── src/
    ├── main/java/com/kafkatutorials/performance/
    │   ├── PerformanceConfigHelper.java   # Configuration factory
    │   ├── BenchmarkResult.java           # Result data class
    │   └── ThroughputBenchmark.java       # Main benchmark runner
    └── test/java/com/kafkatutorials/performance/
        ├── PerformanceBenchmarkTest.java  # Performance tests
        └── CompressionComparisonTest.java # Compression tests
```

## 🚀 Quick Start

### Prerequisites

1. Kafka cluster running (use Docker setup from root):
```bash
make kafka-apache-start
source infra/env.local
```

2. Java 17+ and Gradle installed

### Run the Benchmark Application

```bash
cd java/adv_chapter_02_performance

# Run full benchmark suite
./gradlew runBenchmark

# Or build and run
./gradlew build
./gradlew run
```

### Run Tests

```bash
# Run all performance tests
./gradlew runPerformanceTests

# Run compression comparison tests
./gradlew runCompressionTests

# Run all tests
./gradlew test

# Run specific test
./gradlew test --tests CompressionComparisonTest.testCompareAllCompression
```

## 📊 What Gets Tested

### 1. Performance Profiles

**High Throughput Configuration:**
- Large batch size (64KB)
- Moderate linger time (10ms)
- LZ4 compression
- Multiple in-flight requests
- Optimized for maximum messages/sec

**Low Latency Configuration:**
- Small batch size (1KB)
- No linger time (0ms)
- No compression
- Single in-flight request
- Optimized for minimal latency

**Balanced Configuration:**
- Medium batch size (32KB)
- Short linger time (5ms)
- Snappy compression
- Idempotence enabled
- Good balance of throughput and reliability

### 2. Compression Algorithms

Tests all Kafka compression types:

| Algorithm | Characteristics | Best For |
|-----------|----------------|----------|
| **none** | No compression, baseline | Small messages, high CPU cost |
| **gzip** | Best compression ratio, high CPU | Bandwidth-limited networks |
| **snappy** | Balanced, moderate CPU | General purpose |
| **lz4** | Fast, low CPU | High throughput scenarios |
| **zstd** | Modern, flexible | Latest Kafka versions |

### 3. Batch Size Impact

Tests batch sizes from 1KB to 64KB:
- 1KB: Minimal batching
- 8KB: Small batches
- 16KB: Medium batches
- 32KB: Standard batches
- 64KB: Large batches

### 4. Linger Time Effects

Tests linger times from 0ms to 50ms:
- 0ms: Send immediately
- 5ms: Short wait for batching
- 10ms: Standard wait
- 50ms: Long wait for full batches

## 📈 Sample Output

```
╔═══════════════════════════════════════════════════════════╗
║  Benchmark Results: compression-lz4                       ║
╚═══════════════════════════════════════════════════════════╝

📊 Throughput Metrics:
  • Messages sent:        5,000
  • Failed:               0
  • Duration:             523 ms (0.52 sec)
  • Throughput:           9,560.23 msg/sec
  • Throughput:           9.34 MB/sec

⏱️  Latency Metrics:
  • Average latency:      12 ms
  • P50 latency:          8 ms
  • P95 latency:          35 ms
  • P99 latency:          52 ms
  • Max latency:          89 ms

╔═════════════════════════════════════════════════════════════════════════════╗
║  Performance Comparison Summary                                             ║
╚═════════════════════════════════════════════════════════════════════════════╝

Configuration             |    Throughput   |   Bandwidth   | P95 Latency  | Avg Latency
────────────────────────────────────────────────────────────────────────────────────────
compression-none          |     8,234.12 msg/sec |   8.04 MB/sec |    42 ms |    15 ms
compression-gzip          |     6,123.45 msg/sec |   5.98 MB/sec |    67 ms |    23 ms
compression-snappy        |     8,891.23 msg/sec |   8.68 MB/sec |    38 ms |    14 ms
compression-lz4           |     9,560.23 msg/sec |   9.34 MB/sec |    35 ms |    12 ms
compression-zstd          |     8,456.78 msg/sec |   8.26 MB/sec |    40 ms |    16 ms

🏆 Best Throughput: compression-lz4 (9,560.23 msg/sec)
⚡ Best Latency:    compression-lz4 (35 ms p95)
```

## 🔧 Using the Classes Programmatically

### PerformanceConfigHelper

Factory class for creating producer configurations:

```java
import com.kafkatutorials.performance.PerformanceConfigHelper;

// Get high throughput config
Properties config = PerformanceConfigHelper.getHighThroughputConfig();

// Get low latency config
Properties config = PerformanceConfigHelper.getLowLatencyConfig();

// Get config with specific compression
Properties config = PerformanceConfigHelper.getCompressionConfig("lz4");

// Get config with specific batch size
Properties config = PerformanceConfigHelper.getBatchSizeConfig(32768);

// Print configuration
PerformanceConfigHelper.printConfig(config, "My Config");
```

### ThroughputBenchmark

Main benchmark runner:

```java
import com.kafkatutorials.performance.ThroughputBenchmark;
import com.kafkatutorials.performance.BenchmarkResult;

ThroughputBenchmark benchmark = new ThroughputBenchmark();

// Run single benchmark
Properties config = PerformanceConfigHelper.getHighThroughputConfig();
BenchmarkResult result = benchmark.runBenchmark(
    config,
    "my-topic",
    10000,  // message count
    1024    // message size in bytes
);

result.print();

// Compare multiple configurations
List<BenchmarkResult> results = benchmark.compareConfigs(
    "my-topic",
    10000,
    1024,
    config1, config2, config3
);

ThroughputBenchmark.printComparisonTable(results);

// Compare compression algorithms
List<BenchmarkResult> compressionResults = benchmark.compareCompression(
    "my-topic", 10000, 1024
);

// Compare batch sizes
List<BenchmarkResult> batchResults = benchmark.compareBatchSizes(
    "my-topic", 10000, 1024
);
```

### BenchmarkResult

Results container with metrics:

```java
BenchmarkResult result = benchmark.runBenchmark(...);

// Get metrics
double throughput = result.getThroughputMessagesPerSec();
double bandwidth = result.getThroughputMBPerSec();
long avgLatency = result.getAvgLatencyMs();
long p50 = result.getP50LatencyMs();
long p95 = result.getP95LatencyMs();
long p99 = result.getP99LatencyMs();
long max = result.getMaxLatencyMs();

// Print results
result.print();           // Detailed output
result.printSummary();    // One-line summary
```

## 🧪 Running Specific Tests

```bash
# All performance tests
./gradlew test --tests PerformanceBenchmarkTest

# Specific performance test
./gradlew test --tests PerformanceBenchmarkTest.testHighThroughputConfig
./gradlew test --tests PerformanceBenchmarkTest.testCompareProfiles
./gradlew test --tests PerformanceBenchmarkTest.testBatchingEfficiency

# All compression tests
./gradlew test --tests CompressionComparisonTest

# Specific compression test
./gradlew test --tests CompressionComparisonTest.testCompareAllCompression
./gradlew test --tests CompressionComparisonTest.testCompressionLatencyImpact
```

## 📝 Key Findings from Tests

### Throughput Optimization

1. **Batching is Critical**
   - Small batches (1KB): ~5,000 msg/sec
   - Large batches (64KB): ~10,000 msg/sec
   - **2x improvement** with proper batching

2. **Compression Choices Matter**
   - LZ4: Best throughput (low CPU overhead)
   - Snappy: Good balance
   - GZIP: Best compression ratio (high CPU)
   - Choose based on network vs CPU constraints

3. **Linger Time Sweet Spot**
   - 0ms: Lowest latency, lower throughput
   - 5-10ms: Best balance
   - 50ms+: Higher throughput, unacceptable latency

### Latency Considerations

1. **Low Latency Config**
   - No batching, no compression
   - P95 latency: < 50ms
   - Throughput trade-off: ~40% reduction

2. **Balanced Config**
   - Moderate batching, snappy compression
   - P95 latency: < 100ms
   - Throughput: 80-90% of max

## 🎯 Production Recommendations

### High Throughput Workloads
```properties
batch.size=65536               # 64KB
linger.ms=10                   # Wait for batch
compression.type=lz4           # Fast compression
max.in.flight.requests=5       # Parallel requests
acks=1                         # Balance reliability
```

### Low Latency Requirements
```properties
batch.size=1024                # 1KB
linger.ms=0                    # Send immediately
compression.type=none          # No compression overhead
max.in.flight.requests=1       # Strict ordering
acks=1                         # Fast acknowledgment
```

### Balanced Production Use
```properties
batch.size=32768               # 32KB
linger.ms=5                    # Short wait
compression.type=snappy        # Balanced
max.in.flight.requests=3       # Some parallelism
acks=all                       # Full reliability
enable.idempotence=true        # Exactly-once
```

## 🔍 Troubleshooting

### Tests Running Slowly

**Symptom:** Tests take a long time
**Solution:** 
- Reduce MESSAGE_COUNT in test files
- Ensure Kafka is running locally
- Check network latency

### OutOfMemoryError

**Symptom:** Tests fail with OOM
**Solution:**
- Increase JVM heap: `export GRADLE_OPTS="-Xmx2g"`
- Reduce message count or size
- Check buffer.memory settings

### Connection Refused

**Symptom:** Can't connect to Kafka
**Solution:**
```bash
# Verify Kafka is running
make kafka-apache-status

# Check connection
source infra/env.local
echo $KAFKA_BOOTSTRAP_SERVERS

# Restart Kafka if needed
make kafka-apache-start
```

## 📚 Related Documentation

- **Bash equivalent**: `bash/chapters/adv_chapter_02_performance/`
- **Chapter 01 (Reliability)**: `java/adv_chapter_01_reliability/`
- **Kafka Producer Docs**: https://kafka.apache.org/documentation/#producerconfigs
- **Performance Tuning**: https://kafka.apache.org/documentation/#design_performance

## 🎓 Learning Path

1. **Start Here**: Run `./gradlew runBenchmark` to see default results
2. **Explore Configs**: Review `PerformanceConfigHelper` to understand settings
3. **Run Tests**: Execute `./gradlew test` to see comprehensive comparisons
4. **Experiment**: Modify configurations and re-run benchmarks
5. **Analyze**: Compare results for your specific use case

## 📊 Gradle Tasks Summary

| Task | Description |
|------|-------------|
| `./gradlew build` | Build project |
| `./gradlew test` | Run all tests |
| `./gradlew runBenchmark` | Run standalone benchmark |
| `./gradlew runPerformanceTests` | Run performance tests only |
| `./gradlew runCompressionTests` | Run compression tests only |
| `./gradlew clean` | Clean build artifacts |

---

**Next Steps:**
- Compare results with bash chapter: `bash/chapters/adv_chapter_02_performance/`
- Move to Chapter 03: Keys, Partitioning, and Ordering
- Apply learnings to your production workload
