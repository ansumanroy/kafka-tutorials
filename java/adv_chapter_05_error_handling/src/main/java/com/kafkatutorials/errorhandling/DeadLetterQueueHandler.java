package com.kafkatutorials.errorhandling;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Handles failed messages by sending them to a Dead Letter Queue (DLQ).
 * Enriches DLQ messages with error context and metadata.
 */
public class DeadLetterQueueHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueHandler.class);
    private static final Gson gson = new Gson();
    
    private final KafkaProducer<String, String> dlqProducer;
    private final String dlqTopicSuffix;
    private final ProducerMetrics metrics;
    
    /**
     * Create a DLQ handler.
     * 
     * @param dlqProducer Producer for sending to DLQ
     * @param dlqTopicSuffix Suffix to append to original topic (e.g., ".dlq")
     * @param metrics Metrics collector
     */
    public DeadLetterQueueHandler(
            KafkaProducer<String, String> dlqProducer,
            String dlqTopicSuffix,
            ProducerMetrics metrics) {
        this.dlqProducer = dlqProducer;
        this.dlqTopicSuffix = dlqTopicSuffix;
        this.metrics = metrics;
    }
    
    /**
     * Send a failed record to the DLQ with error context.
     * 
     * @param originalTopic Original topic name
     * @param key Original key
     * @param value Original value
     * @param error The error that occurred
     * @return true if successfully sent to DLQ, false otherwise
     */
    public boolean sendToDlq(String originalTopic, String key, String value, Throwable error) {
        return sendToDlq(originalTopic, key, value, error, 0);
    }
    
    /**
     * Send a failed record to the DLQ with error context and retry count.
     * 
     * @param originalTopic Original topic name
     * @param key Original key
     * @param value Original value
     * @param error The error that occurred
     * @param retryCount Number of retry attempts
     * @return true if successfully sent to DLQ, false otherwise
     */
    public boolean sendToDlq(String originalTopic, String key, String value, 
                            Throwable error, int retryCount) {
        String dlqTopic = originalTopic + dlqTopicSuffix;
        
        try {
            // Create DLQ envelope with metadata
            DlqEnvelope envelope = new DlqEnvelope(
                originalTopic,
                key,
                value,
                error.getClass().getSimpleName(),
                error.getMessage(),
                retryCount,
                Instant.now().toString()
            );
            
            String dlqPayload = gson.toJson(envelope);
            
            ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
                dlqTopic,
                key,  // Preserve original key
                dlqPayload
            );
            
            // Add headers with error metadata
            dlqRecord.headers()
                .add("original-topic", originalTopic.getBytes(StandardCharsets.UTF_8))
                .add("error-type", error.getClass().getName().getBytes(StandardCharsets.UTF_8))
                .add("error-message", 
                     (error.getMessage() != null ? error.getMessage() : "").getBytes(StandardCharsets.UTF_8))
                .add("retry-count", String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8))
                .add("timestamp", Instant.now().toString().getBytes(StandardCharsets.UTF_8));
            
            // Send to DLQ synchronously to ensure it's delivered
            RecordMetadata metadata = dlqProducer.send(dlqRecord).get();
            
            if (metrics != null) {
                metrics.recordDlq(originalTopic);
            }
            
            logger.warn("Sent message to DLQ: topic={}, dlqTopic={}, key={}, errorType={}, retryCount={}", 
                originalTopic, dlqTopic, key, error.getClass().getSimpleName(), retryCount);
            
            return true;
            
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to send message to DLQ: topic={}, dlqTopic={}, error={}", 
                originalTopic, dlqTopic, e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Close the DLQ producer.
     */
    public void close() {
        if (dlqProducer != null) {
            dlqProducer.close();
        }
    }
    
    /**
     * DLQ envelope containing original message and error metadata.
     */
    public static class DlqEnvelope {
        private final String originalTopic;
        private final String originalKey;
        private final String originalValue;
        private final String errorType;
        private final String errorMessage;
        private final int retryCount;
        private final String timestamp;
        
        public DlqEnvelope(String originalTopic, String originalKey, String originalValue,
                          String errorType, String errorMessage, int retryCount, String timestamp) {
            this.originalTopic = originalTopic;
            this.originalKey = originalKey;
            this.originalValue = originalValue;
            this.errorType = errorType;
            this.errorMessage = errorMessage;
            this.retryCount = retryCount;
            this.timestamp = timestamp;
        }
        
        // Getters
        public String getOriginalTopic() { return originalTopic; }
        public String getOriginalKey() { return originalKey; }
        public String getOriginalValue() { return originalValue; }
        public String getErrorType() { return errorType; }
        public String getErrorMessage() { return errorMessage; }
        public int getRetryCount() { return retryCount; }
        public String getTimestamp() { return timestamp; }
    }
}
