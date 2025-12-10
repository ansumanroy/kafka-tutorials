package com.kafkatutorials.reliability;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Helper class to build Kafka producer configurations from environment variables.
 * Mirrors the bash env.sh loader behavior.
 */
public class ProducerConfigHelper {
    
    /**
     * Creates base producer properties from environment variables.
     * Reads from infra/env.msk or infra/env.local files (set as env vars before running).
     */
    public static Properties getBaseProducerConfig() {
        Properties props = new Properties();
        
        // Bootstrap servers (required)
        String bootstrapServers = getEnvOrSystemProperty("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new IllegalStateException(
                "KAFKA_BOOTSTRAP_SERVERS is not set. " +
                "Source infra/env.msk or infra/env.local before running tests."
            );
        }
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Security configuration (optional)
        String securityProtocol = getEnvOrSystemProperty("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
        props.put("security.protocol", securityProtocol);
        
        String saslMechanism = getEnvOrSystemProperty("KAFKA_SASL_MECHANISM");
        if (saslMechanism != null && !saslMechanism.isEmpty()) {
            props.put("sasl.mechanism", saslMechanism);
            
            String username = getEnvOrSystemProperty("KAFKA_SASL_USERNAME");
            String password = getEnvOrSystemProperty("KAFKA_SASL_PASSWORD");
            
            if (username != null && password != null && !username.isEmpty() && !password.isEmpty()) {
                String jaasConfig = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    username, password
                );
                props.put("sasl.jaas.config", jaasConfig);
            }
        }
        
        // Client ID for debugging
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-tutorials-java-reliability");
        
        return props;
    }
    
    /**
     * Creates producer config with high reliability settings.
     * Idempotence, acks=all, retries, etc.
     */
    public static Properties getHighReliabilityConfig() {
        Properties props = getBaseProducerConfig();
        
        // Enable idempotence for exactly-once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Wait for all in-sync replicas
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        // Retry configuration
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        
        // Timeouts
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // 2 minutes
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);   // 30 seconds
        
        // Allow up to 5 in-flight requests while maintaining ordering
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        // Compression for efficiency
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        
        return props;
    }
    
    /**
     * Creates producer config with balanced settings.
     */
    public static Properties getBalancedConfig() {
        Properties props = getBaseProducerConfig();
        
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        return props;
    }
    
    /**
     * Creates producer config for fire-and-forget (low reliability).
     */
    public static Properties getFireAndForgetConfig() {
        Properties props = getBaseProducerConfig();
        
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        
        return props;
    }
    
    /**
     * Helper to get environment variable or system property.
     * Checks System.getProperty first (for test environment), then System.getenv.
     */
    private static String getEnvOrSystemProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            value = System.getenv(key);
        }
        return value;
    }
    
    private static String getEnvOrSystemProperty(String key, String defaultValue) {
        String value = getEnvOrSystemProperty(key);
        return (value == null || value.isEmpty()) ? defaultValue : value;
    }
}

