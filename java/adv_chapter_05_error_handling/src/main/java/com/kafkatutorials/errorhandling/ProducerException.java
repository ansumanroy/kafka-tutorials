package com.kafkatutorials.errorhandling;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Custom exception for producer errors with enhanced context.
 */
public class ProducerException extends Exception {
    
    private final String topic;
    private final Object key;
    private final Object value;
    private final long timestamp;
    private final boolean retryable;
    
    public ProducerException(String message, Throwable cause, ProducerRecord<?, ?> record, boolean retryable) {
        super(message, cause);
        this.topic = record != null ? record.topic() : null;
        this.key = record != null ? record.key() : null;
        this.value = record != null ? record.value() : null;
        this.timestamp = System.currentTimeMillis();
        this.retryable = retryable;
    }
    
    public ProducerException(String message, Throwable cause, String topic, boolean retryable) {
        super(message, cause);
        this.topic = topic;
        this.key = null;
        this.value = null;
        this.timestamp = System.currentTimeMillis();
        this.retryable = retryable;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public Object getKey() {
        return key;
    }
    
    public Object getValue() {
        return value;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public boolean isRetryable() {
        return retryable;
    }
    
    @Override
    public String toString() {
        return String.format(
            "ProducerException{topic='%s', retryable=%s, timestamp=%d, message='%s', cause=%s}",
            topic, retryable, timestamp, getMessage(), getCause()
        );
    }
}
