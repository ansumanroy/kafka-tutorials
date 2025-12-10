package com.kafkatutorials.errorhandling;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A producer wrapper that implements custom retry logic with exponential backoff.
 * Sends failed messages to DLQ after max retries exceeded.
 */
public class RetryableProducer<K, V> implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(RetryableProducer.class);
    
    private final KafkaProducer<K, V> producer;
    private final DeadLetterQueueHandler dlqHandler;
    private final ProducerMetrics metrics;
    private final int maxRetries;
    private final long baseRetryDelayMs;
    private final long maxRetryDelayMs;
    
    /**
     * Create a retryable producer.
     */
    public RetryableProducer(
            Properties producerProps,
            DeadLetterQueueHandler dlqHandler,
            ProducerMetrics metrics,
            int maxRetries,
            long baseRetryDelayMs,
            long maxRetryDelayMs) {
        this.producer = new KafkaProducer<>(producerProps);
        this.dlqHandler = dlqHandler;
        this.metrics = metrics;
        this.maxRetries = maxRetries;
        this.baseRetryDelayMs = baseRetryDelayMs;
        this.maxRetryDelayMs = maxRetryDelayMs;
    }
    
    /**
     * Send a record with automatic retry logic.
     * 
     * @param record The record to send
     * @return RecordMetadata if successful, null if sent to DLQ
     * @throws ProducerException if fatal error occurs
     */
    public RecordMetadata sendWithRetry(ProducerRecord<K, V> record) throws ProducerException {
        int attempt = 0;
        Exception lastException = null;
        
        var timerSample = metrics != null ? metrics.startTimer() : null;
        
        while (attempt <= maxRetries) {
            try {
                // Attempt to send
                RecordMetadata metadata = producer.send(record).get();
                
                // Success!
                if (metrics != null) {
                    metrics.recordSuccess(record.topic(), record.value().toString().getBytes().length);
                    if (timerSample != null) {
                        metrics.stopTimer(timerSample);
                    }
                }
                
                if (attempt > 0) {
                    logger.info("Successfully sent message after {} retries: topic={}, partition={}, offset={}",
                        attempt, metadata.topic(), metadata.partition(), metadata.offset());
                }
                
                return metadata;
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProducerException("Producer interrupted", e, record, false);
                
            } catch (ExecutionException e) {
                lastException = e;
                Exception actualException = (Exception) e.getCause();
                
                // Check if retryable
                boolean isRetryable = ErrorHandlingHelper.isRetryable(actualException);
                boolean isFatal = ErrorHandlingHelper.isFatal(actualException);
                
                if (metrics != null) {
                    metrics.recordFailure(record.topic(), ErrorHandlingHelper.getErrorType(actualException));
                }
                
                // If fatal, send to DLQ immediately
                if (isFatal) {
                    logger.error("Fatal error sending message (attempt {}/{}): {}", 
                        attempt + 1, maxRetries + 1, ErrorHandlingHelper.formatException(actualException));
                    sendToDlqAndThrow(record, actualException, attempt);
                }
                
                // If not retryable and we've exhausted attempts, send to DLQ
                if (!isRetryable || attempt >= maxRetries) {
                    logger.error("Max retries exceeded or non-retryable error: {}", 
                        ErrorHandlingHelper.formatException(actualException));
                    sendToDlqAndThrow(record, actualException, attempt);
                }
                
                // Retry with backoff
                attempt++;
                if (metrics != null) {
                    metrics.recordRetry(record.topic());
                }
                
                long backoffMs = ErrorHandlingHelper.calculateBackoffMs(
                    attempt - 1, baseRetryDelayMs, maxRetryDelayMs);
                
                logger.warn("Retrying message (attempt {}/{}): topic={}, backoff={}ms, error={}",
                    attempt + 1, maxRetries + 1, record.topic(), backoffMs, 
                    ErrorHandlingHelper.formatException(actualException));
                
                try {
                    TimeUnit.MILLISECONDS.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ProducerException("Retry interrupted", ie, record, false);
                }
            }
        }
        
        // Should never reach here, but handle it
        throw new ProducerException(
            "Unexpected error: max retries exceeded",
            lastException,
            record,
            false
        );
    }
    
    /**
     * Send a record to DLQ and throw exception.
     */
    private void sendToDlqAndThrow(ProducerRecord<K, V> record, Exception error, int retryCount) 
            throws ProducerException {
        
        if (dlqHandler != null) {
            String key = record.key() != null ? record.key().toString() : null;
            String value = record.value() != null ? record.value().toString() : null;
            
            boolean dlqSuccess = dlqHandler.sendToDlq(record.topic(), key, value, error, retryCount);
            
            if (!dlqSuccess) {
                logger.error("Failed to send message to DLQ: topic={}", record.topic());
            }
        }
        
        throw new ProducerException(
            "Failed to send message after retries",
            error,
            record,
            false
        );
    }
    
    /**
     * Get the underlying Kafka producer.
     */
    public KafkaProducer<K, V> getProducer() {
        return producer;
    }
    
    /**
     * Flush the producer.
     */
    public void flush() {
        producer.flush();
    }
    
    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
