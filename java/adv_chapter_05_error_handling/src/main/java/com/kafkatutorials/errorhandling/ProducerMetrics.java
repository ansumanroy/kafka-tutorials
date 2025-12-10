package com.kafkatutorials.errorhandling;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and exposes producer metrics for observability.
 * Tracks successes, failures, retries, latencies, and more.
 */
public class ProducerMetrics {
    
    private static final Logger logger = LoggerFactory.getLogger(ProducerMetrics.class);
    
    private final MeterRegistry registry;
    
    // Counters
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter retryCounter;
    private final Counter dlqCounter;
    
    // Timers
    private final Timer sendLatencyTimer;
    
    // Manual tracking
    private final AtomicLong totalBytesSent;
    private final AtomicLong totalRecordsSent;
    private final Map<String, AtomicLong> errorsByType;
    private final Map<String, AtomicLong> recordsByTopic;
    
    public ProducerMetrics() {
        this(new SimpleMeterRegistry());
    }
    
    public ProducerMetrics(MeterRegistry registry) {
        this.registry = registry;
        
        // Initialize counters
        this.successCounter = Counter.builder("kafka.producer.records.success")
            .description("Number of successfully sent records")
            .register(registry);
        
        this.failureCounter = Counter.builder("kafka.producer.records.failure")
            .description("Number of failed records")
            .register(registry);
        
        this.retryCounter = Counter.builder("kafka.producer.records.retry")
            .description("Number of retried records")
            .register(registry);
        
        this.dlqCounter = Counter.builder("kafka.producer.records.dlq")
            .description("Number of records sent to DLQ")
            .register(registry);
        
        // Initialize timer
        this.sendLatencyTimer = Timer.builder("kafka.producer.send.latency")
            .description("Producer send latency")
            .register(registry);
        
        // Manual tracking
        this.totalBytesSent = new AtomicLong(0);
        this.totalRecordsSent = new AtomicLong(0);
        this.errorsByType = new ConcurrentHashMap<>();
        this.recordsByTopic = new ConcurrentHashMap<>();
    }
    
    /**
     * Record a successful send.
     */
    public void recordSuccess(String topic, int bytes) {
        successCounter.increment();
        totalRecordsSent.incrementAndGet();
        totalBytesSent.addAndGet(bytes);
        recordsByTopic.computeIfAbsent(topic, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    /**
     * Record a failed send.
     */
    public void recordFailure(String topic, String errorType) {
        failureCounter.increment();
        errorsByType.computeIfAbsent(errorType, k -> new AtomicLong(0)).incrementAndGet();
        logger.warn("Producer failure recorded: topic={}, errorType={}", topic, errorType);
    }
    
    /**
     * Record a retry attempt.
     */
    public void recordRetry(String topic) {
        retryCounter.increment();
        logger.debug("Producer retry recorded: topic={}", topic);
    }
    
    /**
     * Record a message sent to DLQ.
     */
    public void recordDlq(String topic) {
        dlqCounter.increment();
        logger.info("Message sent to DLQ: originalTopic={}", topic);
    }
    
    /**
     * Record send latency.
     */
    public void recordLatency(long latencyMs) {
        sendLatencyTimer.record(latencyMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Start a timer for send operation.
     */
    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }
    
    /**
     * Stop timer and record latency.
     */
    public void stopTimer(Timer.Sample sample) {
        sample.stop(sendLatencyTimer);
    }
    
    /**
     * Get success count.
     */
    public long getSuccessCount() {
        return (long) successCounter.count();
    }
    
    /**
     * Get failure count.
     */
    public long getFailureCount() {
        return (long) failureCounter.count();
    }
    
    /**
     * Get retry count.
     */
    public long getRetryCount() {
        return (long) retryCounter.count();
    }
    
    /**
     * Get DLQ count.
     */
    public long getDlqCount() {
        return (long) dlqCounter.count();
    }
    
    /**
     * Get total bytes sent.
     */
    public long getTotalBytesSent() {
        return totalBytesSent.get();
    }
    
    /**
     * Get total records sent.
     */
    public long getTotalRecordsSent() {
        return totalRecordsSent.get();
    }
    
    /**
     * Get errors by type.
     */
    public Map<String, Long> getErrorsByType() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        errorsByType.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }
    
    /**
     * Get records by topic.
     */
    public Map<String, Long> getRecordsByTopic() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        recordsByTopic.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }
    
    /**
     * Get average send latency in milliseconds.
     */
    public double getAverageSendLatencyMs() {
        return sendLatencyTimer.mean(TimeUnit.MILLISECONDS);
    }
    
    /**
     * Get max send latency in milliseconds.
     */
    public double getMaxSendLatencyMs() {
        return sendLatencyTimer.max(TimeUnit.MILLISECONDS);
    }
    
    /**
     * Get the meter registry (useful for exposing metrics).
     */
    public MeterRegistry getRegistry() {
        return registry;
    }
    
    /**
     * Print summary of metrics.
     */
    public void printSummary() {
        System.out.println("\n=== Producer Metrics Summary ===");
        System.out.printf("Total Records Sent:     %,d%n", getTotalRecordsSent());
        System.out.printf("Total Bytes Sent:       %,d bytes (%.2f MB)%n", 
            getTotalBytesSent(), getTotalBytesSent() / 1024.0 / 1024.0);
        System.out.printf("Successful Sends:       %,d%n", getSuccessCount());
        System.out.printf("Failed Sends:           %,d%n", getFailureCount());
        System.out.printf("Retried Sends:          %,d%n", getRetryCount());
        System.out.printf("DLQ Sends:              %,d%n", getDlqCount());
        System.out.printf("Avg Send Latency:       %.2f ms%n", getAverageSendLatencyMs());
        System.out.printf("Max Send Latency:       %.2f ms%n", getMaxSendLatencyMs());
        
        if (!getErrorsByType().isEmpty()) {
            System.out.println("\nErrors by Type:");
            getErrorsByType().forEach((type, count) -> 
                System.out.printf("  - %s: %,d%n", type, count));
        }
        
        if (!getRecordsByTopic().isEmpty()) {
            System.out.println("\nRecords by Topic:");
            getRecordsByTopic().forEach((topic, count) -> 
                System.out.printf("  - %s: %,d%n", topic, count));
        }
        
        System.out.println("================================\n");
    }
    
    /**
     * Reset all metrics (useful for testing).
     */
    public void reset() {
        totalBytesSent.set(0);
        totalRecordsSent.set(0);
        errorsByType.clear();
        recordsByTopic.clear();
    }
}
