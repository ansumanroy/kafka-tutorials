package com.kafkatutorials.circuitbreaker;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Kafka producer wrapper with circuit breaker protection.
 * 
 * Provides fail-fast behavior and automatic recovery when Kafka becomes unavailable.
 * 
 * Usage:
 * <pre>{@code
 * CircuitBreakerConfig config = CircuitBreakerConfig.builder()
 *     .failureThreshold(5)
 *     .timeout(Duration.ofSeconds(60))
 *     .build();
 * 
 * CircuitBreakerProducer<String, String> producer = 
 *     new CircuitBreakerProducer<>(producerProps, config);
 * 
 * try {
 *     RecordMetadata metadata = producer.sendWithCircuitBreaker(record);
 *     // Success
 * } catch (CircuitBreakerException e) {
 *     // Circuit is open - request rejected
 * }
 * }</pre>
 */
public class CircuitBreakerProducer<K, V> implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(CircuitBreakerProducer.class);
    
    private final KafkaProducer<K, V> producer;
    private final CircuitBreaker circuitBreaker;
    private final CircuitBreakerMetrics metrics;
    
    /**
     * Create a circuit breaker producer with default configuration.
     */
    public CircuitBreakerProducer(Properties producerProps) {
        this(producerProps, CircuitBreakerConfig.defaultConfig());
    }
    
    /**
     * Create a circuit breaker producer with custom configuration.
     */
    public CircuitBreakerProducer(Properties producerProps, CircuitBreakerConfig config) {
        this(producerProps, config, "kafka-producer");
    }
    
    /**
     * Create a circuit breaker producer with custom configuration and name.
     */
    public CircuitBreakerProducer(Properties producerProps, CircuitBreakerConfig config, String name) {
        this.producer = new KafkaProducer<>(producerProps);
        this.circuitBreaker = new CircuitBreaker(name, config);
        this.metrics = new CircuitBreakerMetrics(name);
        
        logger.info("Created CircuitBreakerProducer '{}' with config: {}", name, config);
    }
    
    /**
     * Send a record synchronously with circuit breaker protection.
     * 
     * @param record The record to send
     * @return RecordMetadata if successful
     * @throws CircuitBreakerException if circuit is open
     * @throws ExecutionException if send fails
     * @throws InterruptedException if interrupted
     */
    public RecordMetadata sendWithCircuitBreaker(ProducerRecord<K, V> record) 
            throws CircuitBreakerException, ExecutionException, InterruptedException {
        
        // Check circuit breaker
        if (!circuitBreaker.allowRequest()) {
            metrics.recordRejected();
            throw new CircuitBreakerException(
                "Circuit breaker is " + circuitBreaker.getState() + " - request rejected",
                circuitBreaker.getState()
            );
        }
        
        metrics.recordAllowed();
        var timerSample = metrics.startTimer();
        
        try {
            // Send to Kafka
            RecordMetadata metadata = producer.send(record).get();
            
            // Record success
            circuitBreaker.recordSuccess();
            metrics.recordSuccess();
            metrics.stopTimer(timerSample);
            
            logger.debug("Successfully sent message to topic '{}', partition {}, offset {}",
                metadata.topic(), metadata.partition(), metadata.offset());
            
            return metadata;
            
        } catch (ExecutionException | InterruptedException e) {
            // Record failure
            circuitBreaker.recordFailure();
            metrics.recordFailure();
            metrics.stopTimer(timerSample);
            
            logger.error("Failed to send message: {}", e.getMessage());
            throw e;
        }
    }
    
    /**
     * Send a record asynchronously with circuit breaker protection.
     * 
     * @param record The record to send
     * @return CompletableFuture with RecordMetadata
     * @throws CircuitBreakerException if circuit is open
     */
    public CompletableFuture<RecordMetadata> sendAsyncWithCircuitBreaker(ProducerRecord<K, V> record) 
            throws CircuitBreakerException {
        
        // Check circuit breaker
        if (!circuitBreaker.allowRequest()) {
            metrics.recordRejected();
            CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
            future.completeExceptionally(new CircuitBreakerException(
                "Circuit breaker is " + circuitBreaker.getState() + " - request rejected",
                circuitBreaker.getState()
            ));
            return future;
        }
        
        metrics.recordAllowed();
        var timerSample = metrics.startTimer();
        
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        
        producer.send(record, (metadata, exception) -> {
            metrics.stopTimer(timerSample);
            
            if (exception != null) {
                circuitBreaker.recordFailure();
                metrics.recordFailure();
                logger.error("Async send failed: {}", exception.getMessage());
                future.completeExceptionally(exception);
            } else {
                circuitBreaker.recordSuccess();
                metrics.recordSuccess();
                logger.debug("Async send successful: topic '{}', partition {}, offset {}",
                    metadata.topic(), metadata.partition(), metadata.offset());
                future.complete(metadata);
            }
        });
        
        return future;
    }
    
    /**
     * Send without circuit breaker protection (direct pass-through).
     * Use this if you want to bypass the circuit breaker for specific messages.
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return producer.send(record);
    }
    
    /**
     * Get the circuit breaker state.
     */
    public CircuitBreakerState getCircuitBreakerState() {
        return circuitBreaker.getState();
    }
    
    /**
     * Check if circuit breaker is closed (accepting requests).
     */
    public boolean isCircuitBreakerClosed() {
        return circuitBreaker.getState() == CircuitBreakerState.CLOSED;
    }
    
    /**
     * Check if circuit breaker is open (rejecting requests).
     */
    public boolean isCircuitBreakerOpen() {
        return circuitBreaker.getState() == CircuitBreakerState.OPEN;
    }
    
    /**
     * Get circuit breaker status.
     */
    public String getCircuitBreakerStatus() {
        return circuitBreaker.getStatus();
    }
    
    /**
     * Reset circuit breaker (for testing/manual intervention).
     */
    public void resetCircuitBreaker() {
        circuitBreaker.reset();
    }
    
    /**
     * Get metrics.
     */
    public CircuitBreakerMetrics getMetrics() {
        return metrics;
    }
    
    /**
     * Print metrics summary.
     */
    public void printMetrics() {
        metrics.printSummary();
    }
    
    /**
     * Flush the producer.
     */
    public void flush() {
        producer.flush();
    }
    
    /**
     * Close the producer.
     */
    @Override
    public void close() {
        logger.info("Closing CircuitBreakerProducer");
        printMetrics();
        System.out.println(getCircuitBreakerStatus());
        producer.close();
    }
}
