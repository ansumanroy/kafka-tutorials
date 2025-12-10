package com.kafkatutorials.partitioning;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates Kafka partitioning behavior with keys.
 * 
 * Shows:
 * - Messages with same key go to same partition
 * - Messages with different keys are distributed
 * - Null keys use round-robin
 * - Ordering is guaranteed within partition
 */
public class PartitioningDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(PartitioningDemo.class);
    
    public static void main(String[] args) {
        String topic = "partitioning-demo-topic";
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Kafka Partitioning Demonstration                         ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");
        System.out.println("\nBootstrap Servers: " + bootstrapServers);
        System.out.println("Topic: " + topic);
        System.out.println();
        
        // Ensure topic exists
        ensureTopicExists(bootstrapServers, topic, 6, (short) 1);
        
        // Run demonstrations
        PartitioningDemo demo = new PartitioningDemo();
        
        demo.demonstrateSameKeyToSamePartition(bootstrapServers, topic);
        demo.demonstrateDifferentKeysDistributed(bootstrapServers, topic);
        demo.demonstrateNullKeyRoundRobin(bootstrapServers, topic);
        demo.demonstrateOrderingGuarantee(bootstrapServers, topic);
        
        // Analyze final distribution
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Final Distribution Analysis                              ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        PartitionDistributionAnalyzer analyzer = 
            new PartitionDistributionAnalyzer(bootstrapServers);
        PartitionDistributionAnalyzer.DistributionReport report = 
            analyzer.analyzeDistribution(topic);
        report.print();
        
        System.out.println("\n✓ Partitioning demonstration complete!");
    }
    
    /**
     * Demonstrate that messages with the same key go to the same partition.
     */
    public void demonstrateSameKeyToSamePartition(String bootstrapServers, String topic) {
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Demo 1: Same Key → Same Partition                       ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        Properties props = PartitioningHelper.getBaseConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        String key = "user-123";
        Set<Integer> partitions = new HashSet<>();
        
        System.out.println("Sending 10 messages with key: " + key);
        
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(topic, key, "message-" + i);
            
            try {
                RecordMetadata metadata = producer.send(record).get();
                partitions.add(metadata.partition());
                
                System.out.printf("  Message %d → Partition %d (offset %d)\n",
                                 i, metadata.partition(), metadata.offset());
            } catch (Exception e) {
                logger.error("Failed to send message", e);
            }
        }
        
        producer.close();
        
        System.out.println("\n✓ Result: All messages went to " + partitions.size() + " partition(s): " + partitions);
        System.out.println("  Expected: 1 partition (same key → same partition)");
    }
    
    /**
     * Demonstrate that messages with different keys are distributed across partitions.
     */
    public void demonstrateDifferentKeysDistributed(String bootstrapServers, String topic) {
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Demo 2: Different Keys → Distributed Partitions         ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        Properties props = PartitioningHelper.getBaseConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        Map<String, Set<Integer>> keyToPartitions = new HashMap<>();
        String[] keys = {"user-A", "user-B", "user-C", "user-D", "user-E"};
        
        System.out.println("Sending messages with different keys:");
        
        for (String key : keys) {
            keyToPartitions.put(key, new HashSet<>());
            
            for (int i = 0; i < 3; i++) {
                ProducerRecord<String, String> record = 
                    new ProducerRecord<>(topic, key, "message-" + i);
                
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    keyToPartitions.get(key).add(metadata.partition());
                    
                    System.out.printf("  Key: %-8s → Partition %d\n",
                                     key, metadata.partition());
                } catch (Exception e) {
                    logger.error("Failed to send message", e);
                }
            }
        }
        
        producer.close();
        
        System.out.println("\n✓ Result: Keys distributed across partitions:");
        keyToPartitions.forEach((key, partitions) -> 
            System.out.printf("  %s → Partition %s\n", key, partitions));
        
        Set<Integer> allPartitions = new HashSet<>();
        keyToPartitions.values().forEach(allPartitions::addAll);
        System.out.println("  Total partitions used: " + allPartitions.size());
    }
    
    /**
     * Demonstrate that null keys use round-robin distribution.
     */
    public void demonstrateNullKeyRoundRobin(String bootstrapServers, String topic) {
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Demo 3: Null Key → Round-Robin Distribution             ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        Properties props = PartitioningHelper.getBaseConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        Map<Integer, Integer> partitionCounts = new HashMap<>();
        
        System.out.println("Sending 12 messages with null keys:");
        
        for (int i = 0; i < 12; i++) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(topic, null, "null-key-message-" + i);
            
            try {
                RecordMetadata metadata = producer.send(record).get();
                partitionCounts.merge(metadata.partition(), 1, Integer::sum);
                
                System.out.printf("  Message %2d → Partition %d\n",
                                 i, metadata.partition());
            } catch (Exception e) {
                logger.error("Failed to send message", e);
            }
        }
        
        producer.close();
        
        System.out.println("\n✓ Result: Messages distributed across partitions:");
        partitionCounts.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> 
                System.out.printf("  Partition %d: %d messages\n",
                                 entry.getKey(), entry.getValue()));
    }
    
    /**
     * Demonstrate that ordering is guaranteed within a partition.
     */
    public void demonstrateOrderingGuarantee(String bootstrapServers, String topic) {
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Demo 4: Ordering Guarantee Within Partition             ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        Properties props = PartitioningHelper.getBaseConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        String key = "order-test";
        List<Long> offsets = new ArrayList<>();
        
        System.out.println("Sending ordered messages with key: " + key);
        
        for (int i = 1; i <= 10; i++) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(topic, key, "order-" + i);
            
            try {
                RecordMetadata metadata = producer.send(record).get();
                offsets.add(metadata.offset());
                
                System.out.printf("  order-%d → Partition %d, Offset %d\n",
                                 i, metadata.partition(), metadata.offset());
            } catch (Exception e) {
                logger.error("Failed to send message", e);
            }
        }
        
        producer.close();
        
        // Check if offsets are sequential
        boolean isSequential = true;
        for (int i = 1; i < offsets.size(); i++) {
            if (offsets.get(i) != offsets.get(i-1) + 1) {
                isSequential = false;
                break;
            }
        }
        
        System.out.println("\n✓ Result:");
        System.out.println("  Offsets: " + offsets);
        System.out.println("  Sequential: " + isSequential);
        System.out.println("  Ordering guaranteed: " + 
                          (isSequential ? "YES ✓" : "NO (unexpected)"));
    }
    
    /**
     * Create topic if it doesn't exist.
     */
    private static void ensureTopicExists(String bootstrapServers, String topicName,
                                         int partitions, short replicationFactor) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
            admin.createTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
            logger.info("Topic created: {}", topicName);
        } catch (Exception e) {
            logger.info("Topic already exists or creation failed: {}", topicName);
        }
    }
}
