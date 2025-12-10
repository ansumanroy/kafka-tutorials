package com.kafkatutorials.partitioning;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Helper class for partitioning-related configurations.
 */
public class PartitioningHelper {
    
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    
    /**
     * Get base producer configuration.
     */
    public static Properties getBaseConfig() {
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS));
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "partitioning-demo");
        
        // For testing, we want predictable behavior
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        return props;
    }
    
    /**
     * Configuration with default partitioner.
     */
    public static Properties getDefaultPartitionerConfig() {
        Properties props = getBaseConfig();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "default-partitioner");
        // Default partitioner is used by default
        return props;
    }
    
    /**
     * Configuration with custom composite key partitioner.
     */
    public static Properties getCompositeKeyPartitionerConfig() {
        Properties props = getBaseConfig();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "composite-partitioner");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, 
                 CompositeKeyPartitioner.class.getName());
        return props;
    }
    
    /**
     * Generate composite key to avoid hot partitions.
     * 
     * @param entityId Base entity ID (e.g., user ID)
     * @param suffix Unique suffix to distribute load
     * @return Composite key
     */
    public static String generateCompositeKey(String entityId, String suffix) {
        return entityId + "-" + suffix;
    }
    
    /**
     * Generate composite key with timestamp suffix.
     */
    public static String generateCompositeKeyWithTimestamp(String entityId) {
        return generateCompositeKey(entityId, String.valueOf(System.nanoTime()));
    }
    
    /**
     * Generate composite key with sequence suffix.
     */
    public static String generateCompositeKeyWithSequence(String entityId, long sequence) {
        return generateCompositeKey(entityId, String.format("%08d", sequence));
    }
}
