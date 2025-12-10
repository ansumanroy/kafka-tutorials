package com.kafkatutorials.performance;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Helper class for creating Kafka producer configurations optimized for performance.
 * 
 * This class provides factory methods for different performance profiles:
 * - High throughput (batching, compression)
 * - Low latency (immediate sends)
 * - Balanced (moderate batching)
 */
public class PerformanceConfigHelper {
    
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    
    /**
     * Get base producer configuration with common settings.
     */
    public static Properties getBaseConfig() {
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS", 
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS));
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "performance-test-producer");
        
        return props;
    }
    
    /**
     * Configuration optimized for maximum throughput.
     * 
     * - Large batch size (64KB)
     * - Moderate linger time (10ms) to fill batches
     * - LZ4 compression for best throughput/compression ratio
     * - Multiple in-flight requests
     * - acks=1 for balance between throughput and reliability
     */
    public static Properties getHighThroughputConfig() {
        Properties props = getBaseConfig();
        
        // Batching settings
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64KB batches
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);     // Wait 10ms to fill batch
        
        // Compression
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        
        // Network
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        // Reliability (balanced)
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        // Buffer memory
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L); // 64MB
        
        return props;
    }
    
    /**
     * Configuration optimized for low latency.
     * 
     * - Small batch size (1KB)
     * - No linger time (send immediately)
     * - No compression (adds CPU overhead)
     * - acks=1 for faster response
     */
    public static Properties getLowLatencyConfig() {
        Properties props = getBaseConfig();
        
        // Minimal batching
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);   // 1KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);       // Send immediately
        
        // No compression
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        
        // Single in-flight request for ordering
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        
        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        return props;
    }
    
    /**
     * Balanced configuration for general use.
     * 
     * - Moderate batch size (32KB)
     * - Short linger time (5ms)
     * - Snappy compression (good balance)
     * - acks=all for reliability
     */
    public static Properties getBalancedConfig() {
        Properties props = getBaseConfig();
        
        // Moderate batching
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);  // 32KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);       // Wait 5ms
        
        // Compression
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        // Network
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 3);
        
        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        return props;
    }
    
    /**
     * Configuration for benchmarking with specific compression.
     */
    public static Properties getCompressionConfig(String compressionType) {
        Properties props = getBaseConfig();
        
        // Standard batching
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);  // 32KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);      // Wait for batch
        
        // Specified compression
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        
        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        return props;
    }
    
    /**
     * Configuration with custom batch size for testing.
     */
    public static Properties getBatchSizeConfig(int batchSizeBytes) {
        Properties props = getBaseConfig();
        
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSizeBytes);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        return props;
    }
    
    /**
     * Configuration with custom linger time for testing.
     */
    public static Properties getLingerConfig(int lingerMs) {
        Properties props = getBaseConfig();
        
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        return props;
    }
    
    /**
     * Print configuration summary for debugging.
     */
    public static void printConfig(Properties props, String configName) {
        System.out.println("\n=== " + configName + " Configuration ===");
        System.out.println("Batch Size:     " + props.getProperty(ProducerConfig.BATCH_SIZE_CONFIG) + " bytes");
        System.out.println("Linger:         " + props.getProperty(ProducerConfig.LINGER_MS_CONFIG) + " ms");
        System.out.println("Compression:    " + props.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        System.out.println("Acks:           " + props.getProperty(ProducerConfig.ACKS_CONFIG));
        System.out.println("In-flight:      " + props.getProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "N/A"));
        System.out.println("Idempotence:    " + props.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false"));
        System.out.println();
    }
}
