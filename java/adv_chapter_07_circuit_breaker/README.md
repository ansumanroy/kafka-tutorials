# Advanced Chapter 07 - Circuit Breaker Pattern (Java)

This Java project demonstrates the Circuit Breaker pattern for Kafka producers, providing fail-fast behavior and automatic recovery to prevent cascading failures.

## 📋 Overview

The Circuit Breaker pattern protects your system from cascading failures by failing fast when a downstream service (Kafka) becomes unavailable. Instead of waiting for timeouts on every request, the circuit breaker "opens" after a threshold of failures and immediately rejects subsequent requests until the service recovers.

### Key Benefits:
- ✅ **Fail Fast**: Instant rejection instead of waiting for timeouts
- ✅ **Resource Protection**: Don't waste threads/connections on failing operations
- ✅ **Cascading Failure Prevention**: Stop failures from spreading through the system
- ✅ **Automatic Recovery**: Self-healing when service restores
- ✅ **Graceful Degradation**: Maintain system stability during outages

## 🏗️ Project Structure

```
adv_chapter_07_circuit_breaker/
├── build.gradle
├── settings.gradle
├── README.md
└── src/
    ├── main/
    │   └── java/com/kafkatutorials/circuitbreaker/
    │       ├── CircuitBreakerState.java         # Enum: CLOSED, OPEN, HALF_OPEN
    │       ├── CircuitBreakerConfig.java        # Configuration builder
    │       ├── CircuitBreaker.java              # Core state machine
    │       ├── CircuitBreakerException.java     # Custom exception
    │       ├── CircuitBreakerMetrics.java       # Metrics collection
    │       ├── CircuitBreakerProducer.java      # Producer wrapper
    │       └── CircuitBreakerDemo.java          # Demo application
    │
    └── test/
        └── java/com/kafkatutorials/circuitbreaker/
            ├── CircuitBreakerTest.java          # Unit tests (9 tests)
            └── CircuitBreakerProducerTest.java  # Producer tests
```

## 🚀 Quick Start

### Prerequisites

```bash
# Start Kafka
make kafka-apache-start

# Set environment
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
```

### Build Project

```bash
cd java/adv_chapter_07_circuit_breaker
gradle build

# Or from root
make java-circuitbreaker-build
```

### Run Demo

```bash
gradle runCircuitBreakerDemo

# Or from root
make java-circuitbreaker-demo
```

### Run Tests

```bash
gradle test

# Or specific tests
gradle test --tests CircuitBreakerTest
```

## 🔄 Circuit Breaker States

### State Diagram

```
┌─────────┐
│ CLOSED  │◄─────────────────────────┐
└────┬────┘                          │
     │                               │
     │ 5 failures                    │ 2 successes
     │                               │
     ▼                               │
┌─────────┐      60s timeout     ┌──┴────────┐
│  OPEN   │─────────────────────▶│ HALF_OPEN │
└─────────┘                      └────┬──────┘
     ▲                                │
     │                                │
     └────────────────────────────────┘
              1 failure
```

### 1. CLOSED (Normal Operation)

```
✅ Requests pass through normally
📊 Count consecutive failures
🔄 Transition to OPEN when threshold exceeded
```

### 2. OPEN (Circuit Opened)

```
❌ Reject all requests immediately
⏱️  Wait for timeout period
🔄 Transition to HALF_OPEN after timeout
```

### 3. HALF_OPEN (Testing Recovery)

```
🔍 Allow limited test requests
📊 Monitor success rate
🔄 Close on success threshold OR reopen on failure
```

## 💻 Usage

### Basic Usage

```java
import com.kafkatutorials.circuitbreaker.*;
import java.time.Duration;

// 1. Create configuration
CircuitBreakerConfig config = CircuitBreakerConfig.builder()
    .failureThreshold(5)           // Open after 5 failures
    .successThreshold(2)           // Close after 2 successes
    .timeout(Duration.ofSeconds(60)) // Try half-open after 60s
    .build();

// 2. Create producer properties
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

// 3. Create circuit breaker producer
CircuitBreakerProducer<String, String> producer = 
    new CircuitBreakerProducer<>(props, config);

// 4. Send messages
ProducerRecord<String, String> record = 
    new ProducerRecord<>("my-topic", "key", "value");

try {
    RecordMetadata metadata = producer.sendWithCircuitBreaker(record);
    System.out.println("Success: " + metadata.offset());
    
} catch (CircuitBreakerException e) {
    System.out.println("REJECTED - Circuit is " + e.getState());
    // Handle rejection (use fallback, cache, etc.)
    
} catch (ExecutionException e) {
    System.out.println("Send failed: " + e.getMessage());
}

// 5. Close producer
producer.close();
```

### Async Usage

```java
CompletableFuture<RecordMetadata> future = 
    producer.sendAsyncWithCircuitBreaker(record);

future
    .thenAccept(metadata -> 
        System.out.println("Success: offset " + metadata.offset()))
    .exceptionally(throwable -> {
        if (throwable instanceof CircuitBreakerException) {
            System.out.println("Circuit breaker rejected request");
        } else {
            System.out.println("Send failed: " + throwable.getMessage());
        }
        return null;
    });
```

### With Fallback Strategies

```java
try {
    RecordMetadata metadata = producer.sendWithCircuitBreaker(record);
    // Success
    
} catch (CircuitBreakerException e) {
    // Circuit open - implement fallback
    
    if (e.getState() == CircuitBreakerState.OPEN) {
        // Option 1: Queue for later
        messageQueue.offer(record);
        
        // Option 2: Send to backup topic
        producer.send(new ProducerRecord<>("backup-topic", record.value()));
        
        // Option 3: Cache locally
        localCache.put(record.key(), record.value());
        
        // Option 4: Return error to client with retry-later
        return Response.status(503).entity("Service temporarily unavailable").build();
    }
}
```

## 🎯 Configuration

### CircuitBreakerConfig Builder

```java
CircuitBreakerConfig config = CircuitBreakerConfig.builder()
    .failureThreshold(5)                  // Consecutive failures before opening
    .successThreshold(2)                  // Successes in half-open to close
    .timeout(Duration.ofSeconds(60))      // Time before trying half-open
    .resetTimeout(Duration.ofSeconds(30)) // Time to reset failure count
    .halfOpenMaxConcurrentRequests(3)     // Max concurrent in half-open
    .build();
```

### Configuration Guidelines

| System Type | Failure Threshold | Timeout | Success Threshold |
|-------------|-------------------|---------|-------------------|
| **High Traffic** | 10 | 30s | 5 |
| **Critical** | 3 | 120s | 10 |
| **Unstable Network** | 5 | 60s | 3 |
| **Development** | 2 | 10s | 1 |

## 📊 Metrics

### Available Metrics

```java
CircuitBreakerMetrics metrics = producer.getMetrics();

// Request metrics
long allowed = metrics.getTotalAllowed();
long rejected = metrics.getTotalRejected();

// Operation metrics
long success = metrics.getTotalSuccess();
long failure = metrics.getTotalFailure();

// Success rate
double rate = metrics.getSuccessRate();

// Latency
double avgLatency = metrics.getAverageOperationTimeMs();

// Print summary
metrics.printSummary();
```

### Sample Output

```
=== Circuit Breaker Metrics: kafka-producer ===
Total Allowed:      85
Total Rejected:     15
Total Success:      80
Total Failure:      5
Success Rate:       94.1%
Avg Operation Time: 12.45 ms
Time Saved:         75,000 ms (75.0 seconds)
================================================
```

### Micrometer Integration

```java
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

PrometheusMeterRegistry prometheusRegistry = 
    new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

CircuitBreakerMetrics metrics = 
    new CircuitBreakerMetrics("my-producer", prometheusRegistry);

// Metrics available at /metrics endpoint
String metricsOutput = prometheusRegistry.scrape();
```

## 🧪 Testing

### Run All Tests

```bash
gradle test
```

### Run Specific Test Class

```bash
gradle test --tests CircuitBreakerTest
```

### Test Coverage

**CircuitBreakerTest** (9 tests):
- ✅ Initial state is CLOSED
- ✅ Circuit opens after threshold
- ✅ Circuit remains closed below threshold
- ✅ Success resets failure count
- ✅ Transition to half-open after timeout
- ✅ Half-open closes after success threshold
- ✅ Half-open reopens on failure
- ✅ Reset functionality
- ✅ Concurrent requests (thread safety)

**CircuitBreakerProducerTest**:
- Configuration validation
- Metrics collection
- Exception handling

## 📈 Performance Impact

### Without Circuit Breaker

```
Scenario: Kafka is down
100 requests × 30s timeout = 50 minutes wasted! 😱

Problems:
- All threads blocked
- Resources exhausted
- System unresponsive
- Poor user experience
```

### With Circuit Breaker

```
Scenario: Kafka is down
5 failures (2.5 min) → Circuit opens
95 rejections (instant) → Fail fast
Total: ~3 minutes

Time Saved: 47 minutes (94% faster!) ⚡

Benefits:
- Threads available
- System responsive
- Fast feedback
- Automatic recovery
```

## 🎓 Production Best Practices

### 1. Monitoring

```java
// Monitor circuit breaker state
if (producer.isCircuitBreakerOpen()) {
    alertOps("Circuit breaker OPEN for Kafka producer");
}

// Track metrics
metrics.printSummary();
```

### 2. Alerting

```java
// Alert on state transitions
circuitBreaker.addStateChangeListener((oldState, newState) -> {
    if (newState == CircuitBreakerState.OPEN) {
        alert("Circuit breaker opened - Kafka may be down");
    } else if (newState == CircuitBreakerState.CLOSED) {
        alert("Circuit breaker closed - Kafka recovered");
    }
});
```

### 3. Graceful Degradation

```java
try {
    producer.sendWithCircuitBreaker(record);
} catch (CircuitBreakerException e) {
    // Fallback strategies
    if (e.getState() == CircuitBreakerState.OPEN) {
        // Queue for later replay
        persistToLocalQueue(record);
        
        // Or return cached/default response
        return getCachedResponse();
    }
}
```

### 4. Testing Failure Scenarios

```java
// Simulate Kafka outage
circuitBreaker.reset();

// Send requests that will fail
for (int i = 0; i < config.getFailureThreshold(); i++) {
    // Requests timeout/fail
}

// Verify circuit opened
assertEquals(CircuitBreakerState.OPEN, circuitBreaker.getState());

// Verify fast rejection
long start = System.currentTimeMillis();
try {
    producer.sendWithCircuitBreaker(record);
} catch (CircuitBreakerException e) {
    long elapsed = System.currentTimeMillis() - start;
    assertTrue(elapsed < 100); // Rejected in < 100ms
}
```

## 🔍 Troubleshooting

### Issue: Circuit Stays Open

**Symptoms:**
- All requests rejected
- Circuit doesn't transition to half-open

**Solutions:**
1. Check timeout configuration
2. Verify Kafka is actually up
3. Check logs for state transitions
4. Manually reset if needed: `producer.resetCircuitBreaker()`

### Issue: Circuit Opens Too Frequently

**Symptoms:**
- Frequent state transitions
- Many rejected requests

**Solutions:**
1. Increase failure threshold
2. Increase timeout duration
3. Check network stability
4. Review Kafka broker health

### Issue: Circuit Doesn't Open

**Symptoms:**
- Continuous timeouts
- Circuit stays closed

**Solutions:**
1. Verify failure threshold is reached
2. Check that failures are being recorded
3. Review exception handling
4. Enable debug logging

## 📚 Related Documentation

- **Bash equivalent**: `bash/chapters/adv_chapter_07_circuit_breaker/`
- **Chapter 01**: Reliability (foundation)
- **Chapter 05**: Error Handling (complementary)
- **Chapter 06**: Operations (production deployment)

## 🎯 Key Takeaways

1. **Fail Fast**: Don't wait for timeouts when service is down
2. **Automatic Recovery**: Circuit tests and recovers automatically  
3. **Prevent Cascading**: Stop failures from spreading through system
4. **Thread Safety**: Use thread-safe implementations in production
5. **Observability**: Monitor state transitions and metrics
6. **Graceful Degradation**: Implement fallback strategies
7. **Testing**: Test failure scenarios thoroughly

---

**Next Steps:**
- Integrate with your Kafka producer
- Configure thresholds for your use case
- Set up monitoring and alerting
- Test failure scenarios

Happy circuit breaking! 🔄
