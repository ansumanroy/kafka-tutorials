# Advanced Chapter 05 - Error Handling & Observability (Java)

This Java project demonstrates comprehensive error handling strategies, Dead Letter Queue (DLQ) patterns, custom retry logic, and observability through metrics collection for Kafka producers.

## 📋 Overview

This chapter covers:
- **Error Classification**: Retriable vs. fatal exceptions
- **Retry Strategies**: Exponential backoff, max retries
- **Dead Letter Queue**: DLQ pattern with error enrichment
- **Metrics Collection**: Success/failure rates, latencies, throughput
- **Callback Handling**: Async error handling
- **Observability**: Structured logging and metrics export
- **Production Patterns**: Circuit breakers, timeouts, graceful degradation

## 🏗️ Project Structure

```
adv_chapter_05_error_handling/
├── build.gradle                    # With Micrometer for metrics
├── settings.gradle
├── README.md
└── src/
    ├── main/
    │   └── java/com/kafkatutorials/errorhandling/
    │       ├── ProducerException.java              # Custom exception
    │       ├── ProducerMetrics.java                # Metrics collector
    │       ├── ErrorHandlingHelper.java            # Config & utilities
    │       ├── DeadLetterQueueHandler.java         # DLQ implementation
    │       ├── RetryableProducer.java              # Producer with retry
    │       ├── ErrorHandlingDemo.java              # Demo application
    │       ├── DeadLetterQueueDemo.java            # DLQ demo
    │       └── MetricsDemo.java                    # Metrics demo
    │
    └── test/java/com/kafkatutorials/errorhandling/
        ├── ErrorHandlingTest.java                  # Error tests
        ├── DeadLetterQueueTest.java                # DLQ tests
        └── MetricsTest.java                        # Metrics tests
```

## 🚀 Quick Start

### Prerequisites

1. **Kafka cluster running:**
```bash
make kafka-apache-start

# Set environment
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
```

### Build Project

```bash
cd java/adv_chapter_05_error_handling
gradle build

# Or from root
make java-errorhandling-build
```

### Run Demos

```bash
# Error handling demo
gradle runErrorDemo
# Or: make java-errorhandling-demo

# DLQ demo
gradle runDlqDemo
# Or: make java-errorhandling-dlq

# Metrics demo
gradle runMetricsDemo
# Or: make java-errorhandling-metrics
```

### Run Tests

```bash
gradle test

# Or specific test suites
gradle runErrorTests          # Error handling tests
gradle runDlqTests            # DLQ tests
gradle runMetricsTests        # Metrics tests
```

## 🔧 Core Components

### 1. ProducerException

Custom exception with enhanced context:

```java
import com.kafkatutorials.errorhandling.ProducerException;

try {
    producer.send(record).get();
} catch (ExecutionException e) {
    throw new ProducerException(
        "Send failed",
        e.getCause(),
        record,
        ErrorHandlingHelper.isRetryable((Exception) e.getCause())
    );
}

// Access context
if (exception.isRetryable()) {
    // Retry logic
}
```

### 2. ProducerMetrics

Comprehensive metrics collection using Micrometer:

```java
import com.kafkatutorials.errorhandling.ProducerMetrics;

// Create metrics collector
ProducerMetrics metrics = new ProducerMetrics();

// Record events
metrics.recordSuccess(topic, messageBytes);
metrics.recordFailure(topic, "TimeoutException");
metrics.recordRetry(topic);
metrics.recordDlq(topic);

// Measure latency
Timer.Sample sample = metrics.startTimer();
// ... send operation ...
metrics.stopTimer(sample);

// Get statistics
long successCount = metrics.getSuccessCount();
long failureCount = metrics.getFailureCount();
double avgLatency = metrics.getAverageSendLatencyMs();

// Print summary
metrics.printSummary();
```

**Output:**
```
=== Producer Metrics Summary ===
Total Records Sent:     10,000
Total Bytes Sent:       1,234,567 bytes (1.18 MB)
Successful Sends:       9,850
Failed Sends:           150
Retried Sends:          75
DLQ Sends:              25
Avg Send Latency:       12.34 ms
Max Send Latency:       245.67 ms

Errors by Type:
  - TimeoutException: 120
  - NetworkException: 30

Records by Topic:
  - user-events: 8,000
  - order-events: 2,000
================================
```

### 3. ErrorHandlingHelper

Utilities for error classification and handling:

```java
import com.kafkatutorials.errorhandling.ErrorHandlingHelper;

// Get producer config
Properties props = ErrorHandlingHelper.getErrorAwareProducerConfig();
// - acks=all
// - retries=3
// - idempotence=true
// - request.timeout.ms=30000

// Error classification
Exception error = ...;

if (ErrorHandlingHelper.isRetryable(error)) {
    // Retry: TimeoutException, NetworkException, etc.
    long backoffMs = ErrorHandlingHelper.calculateBackoffMs(
        retryAttempt, 
        1000,  // base delay
        30000  // max delay
    );
    Thread.sleep(backoffMs);
    retry();
    
} else if (ErrorHandlingHelper.isFatal(error)) {
    // Fatal: RecordTooLargeException, AuthorizationException, etc.
    sendToDlq();
    
} else {
    // Unknown - log and decide
}

// Format for logging
String formatted = ErrorHandlingHelper.formatException(error);
logger.error("Send failed: {}", formatted);
```

### 4. DeadLetterQueueHandler

Send failed messages to DLQ with enriched metadata:

```java
import com.kafkatutorials.errorhandling.DeadLetterQueueHandler;

// Create DLQ handler
KafkaProducer<String, String> dlqProducer = new KafkaProducer<>(
    ErrorHandlingHelper.getDlqProducerConfig()
);

DeadLetterQueueHandler dlqHandler = new DeadLetterQueueHandler(
    dlqProducer,
    ".dlq",      // suffix for DLQ topics
    metrics
);

// Send to DLQ
boolean success = dlqHandler.sendToDlq(
    "user-events",           // original topic
    "user-123",              // original key
    originalMessageJson,     // original value
    exception,               // error that occurred
    3                        // retry count
);

// DLQ message structure:
// {
//   "originalTopic": "user-events",
//   "originalKey": "user-123",
//   "originalValue": "{...}",
//   "errorType": "TimeoutException",
//   "errorMessage": "Request timed out",
//   "retryCount": 3,
//   "timestamp": "2024-01-15T10:30:00Z"
// }

// Plus headers:
// - original-topic
// - error-type
// - error-message
// - retry-count
// - timestamp
```

### 5. RetryableProducer

Producer wrapper with automatic retry logic:

```java
import com.kafkatutorials.errorhandling.RetryableProducer;

// Create retryable producer
Properties props = ErrorHandlingHelper.getErrorAwareProducerConfig();

RetryableProducer<String, String> producer = new RetryableProducer<>(
    props,
    dlqHandler,         // DLQ handler
    metrics,            // Metrics collector
    3,                  // max retries
    1000,               // base retry delay (ms)
    30000               // max retry delay (ms)
);

// Send with automatic retry
ProducerRecord<String, String> record = new ProducerRecord<>(
    "user-events", 
    "user-123", 
    userJson
);

try {
    RecordMetadata metadata = producer.sendWithRetry(record);
    logger.info("Sent: partition={}, offset={}", 
        metadata.partition(), metadata.offset());
        
} catch (ProducerException e) {
    // All retries exhausted, sent to DLQ
    logger.error("Failed after retries: {}", e.getMessage());
}
```

**Retry Flow:**
```
Attempt 1: Send → Timeout → Retry with 1000ms backoff
Attempt 2: Send → Timeout → Retry with 2000ms backoff
Attempt 3: Send → Timeout → Retry with 4000ms backoff
Attempt 4: Send → Timeout → Send to DLQ → Throw exception
```

## 🎯 Error Handling Patterns

### Pattern 1: Synchronous Send with Try-Catch

```java
KafkaProducer<String, String> producer = new KafkaProducer<>(props);

try {
    RecordMetadata metadata = producer.send(record).get();
    metrics.recordSuccess(record.topic(), recordSize);
    logger.info("Sent successfully: {}", metadata);
    
} catch (ExecutionException e) {
    Throwable cause = e.getCause();
    metrics.recordFailure(record.topic(), ErrorHandlingHelper.getErrorType(cause));
    
    if (ErrorHandlingHelper.isFatal((Exception) cause)) {
        dlqHandler.sendToDlq(record.topic(), key, value, cause, 0);
    } else {
        // Implement custom retry logic
    }
    
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    logger.error("Send interrupted", e);
}
```

### Pattern 2: Asynchronous Send with Callback

```java
producer.send(record, (metadata, exception) -> {
    if (exception != null) {
        // Handle error
        metrics.recordFailure(record.topic(), ErrorHandlingHelper.getErrorType(exception));
        
        if (ErrorHandlingHelper.isRetryable((Exception) exception)) {
            // Queue for retry
            retryQueue.add(record);
        } else {
            // Send to DLQ
            dlqHandler.sendToDlq(record.topic(), key, value, exception, 0);
        }
        
    } else {
        // Success
        metrics.recordSuccess(record.topic(), recordSize);
        logger.debug("Sent: partition={}, offset={}", 
            metadata.partition(), metadata.offset());
    }
});
```

### Pattern 3: Batch Processing with Error Collection

```java
List<ProducerRecord<String, String>> batch = ...;
List<ProducerException> errors = new ArrayList<>();

for (ProducerRecord<String, String> record : batch) {
    try {
        producer.sendWithRetry(record);
    } catch (ProducerException e) {
        errors.add(e);
    }
}

// Report batch results
if (errors.isEmpty()) {
    logger.info("Batch completed successfully: {} records", batch.size());
} else {
    logger.error("Batch completed with {} errors", errors.size());
    // Notify monitoring system
}
```

### Pattern 4: Circuit Breaker

```java
public class CircuitBreakerProducer {
    private final RetryableProducer<String, String> producer;
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final int failureThreshold = 10;
    private volatile boolean circuitOpen = false;
    
    public void send(ProducerRecord<String, String> record) throws ProducerException {
        if (circuitOpen) {
            throw new ProducerException("Circuit breaker is OPEN", null, record, false);
        }
        
        try {
            producer.sendWithRetry(record);
            consecutiveFailures.set(0);  // Reset on success
            
        } catch (ProducerException e) {
            int failures = consecutiveFailures.incrementAndGet();
            
            if (failures >= failureThreshold) {
                circuitOpen = true;
                logger.error("Circuit breaker OPEN after {} failures", failures);
                scheduleCircuitReset();
            }
            
            throw e;
        }
    }
    
    private void scheduleCircuitReset() {
        // Reset circuit after cooldown period
        scheduler.schedule(() -> {
            circuitOpen = false;
            consecutiveFailures.set(0);
            logger.info("Circuit breaker CLOSED");
        }, 60, TimeUnit.SECONDS);
    }
}
```

## 📊 Metrics & Observability

### Micrometer Integration

```java
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

// Use Prometheus registry for export
PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(
    PrometheusConfig.DEFAULT
);

ProducerMetrics metrics = new ProducerMetrics(prometheusRegistry);

// Expose metrics endpoint
String metricsOutput = prometheusRegistry.scrape();
System.out.println(metricsOutput);
```

**Prometheus Output:**
```
# HELP kafka_producer_records_success_total Number of successfully sent records
# TYPE kafka_producer_records_success_total counter
kafka_producer_records_success_total 9850.0

# HELP kafka_producer_records_failure_total Number of failed records
# TYPE kafka_producer_records_failure_total counter
kafka_producer_records_failure_total 150.0

# HELP kafka_producer_send_latency_seconds Producer send latency
# TYPE kafka_producer_send_latency_seconds summary
kafka_producer_send_latency_seconds_count 10000.0
kafka_producer_send_latency_seconds_sum 123.45
```

### Structured Logging

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

Logger logger = LoggerFactory.getLogger(MyProducer.class);

// Add context to logs
MDC.put("topic", record.topic());
MDC.put("key", record.key());
MDC.put("correlationId", correlationId);

try {
    producer.send(record).get();
    logger.info("Message sent successfully");
    
} catch (Exception e) {
    logger.error("Failed to send message", e);
    
} finally {
    MDC.clear();
}
```

**Log Output (JSON format):**
```json
{
  "timestamp": "2024-01-15T10:30:00.123Z",
  "level": "ERROR",
  "logger": "MyProducer",
  "message": "Failed to send message",
  "topic": "user-events",
  "key": "user-123",
  "correlationId": "abc-123",
  "exception": "org.apache.kafka.common.errors.TimeoutException: Request timed out"
}
```

## 🎓 Exception Types

### Retriable Exceptions

These can be retried with backoff:

| Exception | Cause | Action |
|-----------|-------|--------|
| `TimeoutException` | Network delay, broker overload | Retry with backoff |
| `NotEnoughReplicasException` | ISR too small | Wait and retry |
| `NetworkException` | Connection issue | Retry |
| `LeaderNotAvailableException` | Leader election | Wait and retry |

### Fatal Exceptions

These should NOT be retried:

| Exception | Cause | Action |
|-----------|-------|--------|
| `RecordTooLargeException` | Message exceeds limit | Send to DLQ, fix message |
| `SerializationException` | Invalid data format | Send to DLQ, fix serializer |
| `AuthorizationException` | Permission denied | Alert, fix ACLs |
| `InvalidTopicException` | Topic doesn't exist | Create topic or alert |
| `UnknownTopicOrPartitionException` | Topic/partition missing | Create or alert |

## 🧪 Testing Error Scenarios

```java
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Test
@Tag("error-handling")
void testRetriableError() throws Exception {
    // Mock producer that fails then succeeds
    KafkaProducer<String, String> mockProducer = Mockito.mock(KafkaProducer.class);
    
    Mockito.when(mockProducer.send(Mockito.any()))
        .thenReturn(failedFuture(new TimeoutException()))  // Fail first
        .thenReturn(successFuture());                       // Then succeed
    
    RetryableProducer<String, String> retryableProducer = new RetryableProducer<>(
        mockProducer, dlqHandler, metrics, 3, 100, 1000
    );
    
    RecordMetadata result = retryableProducer.sendWithRetry(record);
    
    assertNotNull(result);
    assertEquals(1, metrics.getRetryCount());
    assertEquals(1, metrics.getSuccessCount());
}

@Test
@Tag("dlq")
void testFatalErrorSendsToDlq() {
    // Test that fatal errors go directly to DLQ
    Exception fatalError = new RecordTooLargeException();
    
    assertThrows(ProducerException.class, () -> {
        retryableProducer.sendWithRetry(record);
    });
    
    assertEquals(1, metrics.getDlqCount());
    assertEquals(0, metrics.getRetryCount());
}
```

## 🎯 Production Recommendations

### 1. Configure Appropriate Timeouts

```java
props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);    // 30s
props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);  // 2m
props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);          // 1m
```

### 2. Enable Idempotence

```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
```

### 3. Set Retry Limits

```java
// Built-in retries
props.put(ProducerConfig.RETRIES_CONFIG, 3);
props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

// Application-level retries
RetryableProducer producer = new RetryableProducer<>(
    props, dlqHandler, metrics,
    maxRetries: 3,
    baseRetryDelayMs: 1000,
    maxRetryDelayMs: 30000
);
```

### 4. Monitor Key Metrics

```java
// Track these metrics in production:
- Success rate (target: >99.9%)
- Failure rate (alert if >0.1%)
- Average latency (target: <50ms)
- P99 latency (target: <200ms)
- Retry count (alert if >10/min)
- DLQ count (alert if >0/min)
- Circuit breaker status
```

### 5. Set Up Alerting

```yaml
# Example: Prometheus alerts
alerts:
  - alert: HighProducerFailureRate
    expr: rate(kafka_producer_records_failure_total[5m]) > 0.01
    annotations:
      summary: "High producer failure rate"
      
  - alert: HighProducerLatency
    expr: kafka_producer_send_latency_seconds{quantile="0.99"} > 0.2
    annotations:
      summary: "P99 latency > 200ms"
      
  - alert: DLQMessagesDetected
    expr: increase(kafka_producer_records_dlq_total[5m]) > 0
    annotations:
      summary: "Messages being sent to DLQ"
```

### 6. Implement Graceful Shutdown

```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    logger.info("Shutting down producer...");
    
    try {
        // Flush pending messages
        producer.flush();
        logger.info("Flushed pending messages");
        
        // Close producer
        producer.close();
        logger.info("Producer closed");
        
        // Close DLQ handler
        dlqHandler.close();
        logger.info("DLQ handler closed");
        
        // Print final metrics
        metrics.printSummary();
        
    } catch (Exception e) {
        logger.error("Error during shutdown", e);
    }
}));
```

## 🔍 Troubleshooting

### Issue: High Retry Rate

**Symptoms:**
- `metrics.getRetryCount()` increasing rapidly
- Logs showing repeated `TimeoutException`

**Diagnosis:**
```bash
# Check broker health
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Check producer metrics
jconsole # Connect and check producer metrics
```

**Solutions:**
1. Increase timeouts
2. Check network latency
3. Scale up brokers
4. Reduce batch size

### Issue: Messages Going to DLQ

**Symptoms:**
- `metrics.getDlqCount()` > 0
- DLQ topic has messages

**Diagnosis:**
```bash
# Read DLQ messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic user-events.dlq \
  --from-beginning \
  --property print.headers=true

# Check error patterns
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic user-events.dlq \
  --from-beginning | \
  jq '.errorType' | sort | uniq -c
```

**Solutions:**
1. Fix data validation
2. Increase message size limit
3. Fix serialization errors
4. Review ACLs

### Issue: Circuit Breaker Open

**Symptoms:**
- All sends failing immediately
- Logs: "Circuit breaker is OPEN"

**Actions:**
1. Check broker availability
2. Wait for cooldown period
3. Investigate root cause
4. Consider manual reset

## 📚 Related Documentation

- **Bash equivalent**: `bash/chapters/adv_chapter_05_error_handling/` (to be created)
- **Kafka Error Handling**: https://kafka.apache.org/documentation/#producerconfigs
- **Micrometer Docs**: https://micrometer.io/docs

## 🎓 Key Takeaways

1. **Classify Errors**: Retriable vs. fatal
2. **Retry Smart**: Exponential backoff, max retries
3. **Use DLQ**: Preserve failed messages for analysis
4. **Monitor Everything**: Success rate, latency, errors
5. **Structure Logs**: Add context for debugging
6. **Handle Async**: Always use callbacks
7. **Test Failures**: Simulate error scenarios
8. **Alert Proactively**: Don't wait for users to report

---

**Next Steps:**
- Move to Chapter 06: Operational Concerns
- Implement custom error handlers
- Set up monitoring dashboard

Happy error handling! 🛡️
