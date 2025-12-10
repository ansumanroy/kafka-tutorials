package com.kafkatutorials.errorhandling;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Helper class for error handling configurations and utilities.
 */
public class ErrorHandlingHelper {
    
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    
    /**
     * Get base producer configuration with error handling settings.
     */
    public static Properties getErrorAwareProducerConfig() {
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS));
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Error handling configs
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Timeouts
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        
        // For error scenarios
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        
        return props;
    }
    
    /**
     * Get DLQ producer configuration (simpler, fail-fast).
     */
    public static Properties getDlqProducerConfig() {
        Properties props = getErrorAwareProducerConfig();
        
        // DLQ should be simpler - don't retry as much
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.ACKS_CONFIG, "1");  // Faster acknowledgment
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        
        return props;
    }
    
    /**
     * Check if an exception is retryable.
     */
    public static boolean isRetryable(Exception e) {
        if (e == null) {
            return false;
        }
        
        // Check if it's a Kafka retriable exception
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof RetriableException) {
                return true;
            }
            
            // Specific retriable exceptions
            if (cause instanceof TimeoutException ||
                cause instanceof NotEnoughReplicasException ||
                cause instanceof NotEnoughReplicasAfterAppendException ||
                cause instanceof NetworkException) {
                return true;
            }
            
            cause = cause.getCause();
        }
        
        return false;
    }
    
    /**
     * Check if an exception is fatal (should not retry).
     */
    public static boolean isFatal(Exception e) {
        if (e == null) {
            return false;
        }
        
        Throwable cause = e;
        while (cause != null) {
            // Fatal exceptions
            if (cause instanceof RecordTooLargeException ||
                cause instanceof InvalidTopicException ||
                cause instanceof UnknownTopicOrPartitionException ||
                cause instanceof SerializationException ||
                cause instanceof AuthorizationException ||
                cause instanceof AuthenticationException) {
                return true;
            }
            
            cause = cause.getCause();
        }
        
        return false;
    }
    
    /**
     * Get a human-readable error type from an exception.
     */
    public static String getErrorType(Throwable e) {
        if (e == null) {
            return "UNKNOWN";
        }
        
        // Walk the exception chain to find a Kafka exception
        Throwable current = e;
        while (current != null) {
            String className = current.getClass().getSimpleName();
            
            // Return the first meaningful Kafka exception
            if (className.contains("Kafka") || 
                className.contains("Exception") && !className.equals("ExecutionException")) {
                return className;
            }
            
            current = current.getCause();
        }
        
        return e.getClass().getSimpleName();
    }
    
    /**
     * Format an exception for logging.
     */
    public static String formatException(Throwable e) {
        if (e == null) {
            return "null";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append(e.getClass().getSimpleName());
        
        if (e.getMessage() != null) {
            sb.append(": ").append(e.getMessage());
        }
        
        // Add cause if different
        if (e.getCause() != null && e.getCause() != e) {
            sb.append(" (caused by: ").append(formatException(e.getCause())).append(")");
        }
        
        return sb.toString();
    }
    
    /**
     * Calculate exponential backoff delay.
     */
    public static long calculateBackoffMs(int retryAttempt, long baseDelayMs, long maxDelayMs) {
        long delay = (long) (baseDelayMs * Math.pow(2, retryAttempt));
        return Math.min(delay, maxDelayMs);
    }
    
    /**
     * Print configuration summary.
     */
    public static void printConfig(Properties props, String configName) {
        System.out.println("\n=== " + configName + " Configuration ===");
        System.out.println("Bootstrap Servers: " + props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println("Acks:              " + props.getProperty(ProducerConfig.ACKS_CONFIG));
        System.out.println("Retries:           " + props.getProperty(ProducerConfig.RETRIES_CONFIG));
        System.out.println("Idempotence:       " + props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        System.out.println("Request Timeout:   " + props.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG) + " ms");
        System.out.println("Delivery Timeout:  " + props.getProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG) + " ms");
        System.out.println();
    }
}
