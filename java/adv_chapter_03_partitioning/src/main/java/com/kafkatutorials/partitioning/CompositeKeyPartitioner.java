package com.kafkatutorials.partitioning;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Custom partitioner that uses composite keys to avoid hot partitions.
 * 
 * For keys in format "entity-ID-SUFFIX", this partitioner:
 * - Uses the full composite key for partitioning (not just entity ID)
 * - Helps distribute load for hot entities across multiple partitions
 * 
 * Example: "celebrity-user-1234-abc" will hash the full string,
 * spreading "celebrity-user-1234" messages across different partitions
 * based on the suffix.
 * 
 * Usage:
 * <pre>
 * props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, 
 *           CompositeKeyPartitioner.class.getName());
 * </pre>
 */
public class CompositeKeyPartitioner implements Partitioner {
    
    private static final Logger logger = LoggerFactory.getLogger(CompositeKeyPartitioner.class);
    
    @Override
    public void configure(Map<String, ?> configs) {
        logger.info("Configuring CompositeKeyPartitioner");
    }
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, 
                        Object value, byte[] valueBytes, Cluster cluster) {
        
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        
        if (keyBytes == null) {
            // Null key - use round-robin (handled by Kafka)
            return 0; // Kafka will override this
        }
        
        String keyString = key.toString();
        
        // Use full composite key for hashing
        // This ensures "entity-123-A" and "entity-123-B" go to different partitions
        int partition = Math.abs(keyString.hashCode()) % numPartitions;
        
        logger.debug("Partitioning key='{}' to partition {} of {}", 
                    keyString, partition, numPartitions);
        
        return partition;
    }
    
    @Override
    public void close() {
        logger.info("Closing CompositeKeyPartitioner");
    }
}
