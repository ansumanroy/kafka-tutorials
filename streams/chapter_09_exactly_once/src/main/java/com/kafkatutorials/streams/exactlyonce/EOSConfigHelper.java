package com.kafkatutorials.streams.exactlyonce;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Configuration helper for Exactly-Once Semantics (EOS).
 */
public class EOSConfigHelper {
    
    /**
     * Creates configuration with exactly-once semantics enabled.
     * 
     * Exactly-Once Semantics (EOS) ensures:
     * 1. No duplicate processing
     * 2. No lost messages
     * 3. Atomic read-process-write
     */
    public static Properties getEOSConfig(String applicationId, String bootstrapServers) {
        Properties props = new Properties();
        
        // Basic config
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // Enable exactly-once semantics v2 (recommended)
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        
        // Recommended EOS settings
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);  // For production
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);  // Commit every 10 seconds
        
        return props;
    }
    
    /**
     * At-least-once configuration (default).
     * Simpler but may have duplicates on failure.
     */
    public static Properties getAtLeastOnceConfig(String applicationId, String bootstrapServers) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // Default: at-least-once
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        
        return props;
    }
}
