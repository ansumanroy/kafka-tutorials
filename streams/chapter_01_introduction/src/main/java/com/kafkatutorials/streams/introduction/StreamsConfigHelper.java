package com.kafkatutorials.streams.introduction;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Helper class for creating Kafka Streams configurations.
 * Provides reusable configuration templates for different use cases.
 */
public class StreamsConfigHelper {
    
    /**
     * Creates a basic Kafka Streams configuration.
     * 
     * @param applicationId Unique identifier for the streams application
     * @param bootstrapServers Kafka bootstrap servers
     * @return Properties object with basic streams configuration
     */
    public static Properties getBasicConfig(String applicationId, String bootstrapServers) {
        Properties props = new Properties();
        
        // Required: Application ID (used for consumer group, state directory, changelog topics)
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        
        // Required: Bootstrap servers
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        // Default SerDes for keys and values
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        return props;
    }
    
    /**
     * Creates a development-friendly Kafka Streams configuration.
     * Includes settings useful for local development and testing.
     * 
     * @param applicationId Unique identifier for the streams application
     * @param bootstrapServers Kafka bootstrap servers
     * @return Properties object with development-friendly configuration
     */
    public static Properties getDevConfig(String applicationId, String bootstrapServers) {
        Properties props = getBasicConfig(applicationId, bootstrapServers);
        
        // Development settings
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L); // Disable caching for immediate results
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);      // Commit every 1 second
        
        // State directory (useful for cleanup during development)
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        
        return props;
    }
    
    /**
     * Creates a production-ready Kafka Streams configuration.
     * Includes settings for reliability and performance.
     * 
     * @param applicationId Unique identifier for the streams application
     * @param bootstrapServers Kafka bootstrap servers
     * @return Properties object with production configuration
     */
    public static Properties getProductionConfig(String applicationId, String bootstrapServers) {
        Properties props = getBasicConfig(applicationId, bootstrapServers);
        
        // Production settings
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        
        // Performance tuning
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10L * 1024 * 1024); // 10 MB
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000); // 30 seconds
        
        // Topology optimization
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        
        return props;
    }
    
    /**
     * Gets bootstrap servers from environment variable or defaults to localhost.
     * 
     * @return Bootstrap servers string
     */
    public static String getBootstrapServers() {
        return System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
            ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
            : "localhost:9092";
    }
}
