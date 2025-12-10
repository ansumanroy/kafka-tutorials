package com.kafkatutorials.partitioning;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Kafka partitioning behavior.
 * 
 * Demonstrates and verifies:
 * - Same key → same partition
 * - Different keys → distributed
 * - Null keys → round-robin
 * - Ordering within partition
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("partitioning")
class PartitioningBehaviorTest {
    
    private static final Logger logger = LoggerFactory.getLogger(PartitioningBehaviorTest.class);
    private static final String TOPIC_NAME = "partition-behavior-test";
    
    @BeforeEach
    void setUp() {
        ensureTopicExists();
    }
    
    private void ensureTopicExists() {
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic topic = new NewTopic(TOPIC_NAME, 6, (short) 1);
            admin.createTopics(Collections.singletonList(topic)).all().get(10, TimeUnit.SECONDS);
            logger.info("Topic created: {}", TOPIC_NAME);
        } catch (Exception e) {
            logger.info("Topic already exists: {}", TOPIC_NAME);
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("Same key goes to same partition")
    void testSameKeyToSamePartition() {
        logger.info("Testing same key → same partition");
        
        Properties props = PartitioningHelper.getBaseConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        String key = "test-user-123";
        Set<Integer> partitions = new HashSet<>();
        
        // Send 20 messages with the same key
        for (int i = 0; i < 20; i++) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(TOPIC_NAME, key, "message-" + i);
            
            try {
                RecordMetadata metadata = producer.send(record).get();
                partitions.add(metadata.partition());
                
                logger.debug("Message {} sent to partition {}", i, metadata.partition());
            } catch (Exception e) {
                fail("Failed to send message: " + e.getMessage());
            }
        }
        
        producer.close();
        
        // All messages should go to exactly 1 partition
        assertEquals(1, partitions.size(), 
                    "All messages with same key should go to the same partition");
        
        logger.info("✓ All 20 messages went to partition: {}", partitions.iterator().next());
    }
    
    @Test
    @Order(2)
    @DisplayName("Different keys are distributed across partitions")
    void testDifferentKeysDistributed() {
        logger.info("Testing different keys → distributed partitions");
        
        Properties props = PartitioningHelper.getBaseConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        Map<String, Integer> keyToPartition = new HashMap<>();
        String[] keys = {"user-A", "user-B", "user-C", "user-D", "user-E", "user-F"};
        
        // Send messages with different keys
        for (String key : keys) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(TOPIC_NAME, key, "test-value");
            
            try {
                RecordMetadata metadata = producer.send(record).get();
                keyToPartition.put(key, metadata.partition());
                
                logger.debug("Key {} sent to partition {}", key, metadata.partition());
            } catch (Exception e) {
                fail("Failed to send message: " + e.getMessage());
            }
        }
        
        producer.close();
        
        // Check that keys are distributed (at least 2 different partitions used)
        Set<Integer> usedPartitions = new HashSet<>(keyToPartition.values());
        assertTrue(usedPartitions.size() >= 2, 
                  "Different keys should use multiple partitions. Used: " + usedPartitions.size());
        
        logger.info("✓ Keys distributed across {} partitions", usedPartitions.size());
        keyToPartition.forEach((key, partition) -> 
            logger.info("  {} → Partition {}", key, partition));
    }
    
    @Test
    @Order(3)
    @DisplayName("Null keys use round-robin distribution")
    void testNullKeyRoundRobin() {
        logger.info("Testing null key → round-robin");
        
        Properties props = PartitioningHelper.getBaseConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        Map<Integer, Integer> partitionCounts = new HashMap<>();
        int messageCount = 30;
        
        // Send messages with null keys
        for (int i = 0; i < messageCount; i++) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(TOPIC_NAME, null, "null-key-message-" + i);
            
            try {
                RecordMetadata metadata = producer.send(record).get();
                partitionCounts.merge(metadata.partition(), 1, Integer::sum);
            } catch (Exception e) {
                fail("Failed to send message: " + e.getMessage());
            }
        }
        
        producer.close();
        
        // Null keys should be distributed across multiple partitions
        assertTrue(partitionCounts.size() >= 3, 
                  "Null keys should use multiple partitions. Used: " + partitionCounts.size());
        
        logger.info("✓ Null keys distributed across {} partitions", partitionCounts.size());
        partitionCounts.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> 
                logger.info("  Partition {}: {} messages", entry.getKey(), entry.getValue()));
    }
    
    @Test
    @Order(4)
    @DisplayName("Ordering is guaranteed within partition")
    void testOrderingGuarantee() {
        logger.info("Testing ordering guarantee within partition");
        
        Properties props = PartitioningHelper.getBaseConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        String key = "ordering-test-key";
        List<Long> offsets = new ArrayList<>();
        int partition = -1;
        
        // Send ordered messages
        for (int i = 0; i < 15; i++) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(TOPIC_NAME, key, "order-" + i);
            
            try {
                RecordMetadata metadata = producer.send(record).get();
                offsets.add(metadata.offset());
                
                if (partition == -1) {
                    partition = metadata.partition();
                }
                
                // All messages should go to the same partition
                assertEquals(partition, metadata.partition(), 
                           "All messages with same key should go to same partition");
                
                logger.debug("Message {} sent to partition {}, offset {}", 
                           i, metadata.partition(), metadata.offset());
            } catch (Exception e) {
                fail("Failed to send message: " + e.getMessage());
            }
        }
        
        producer.close();
        
        // Verify offsets are sequential (ordering preserved)
        for (int i = 1; i < offsets.size(); i++) {
            long expected = offsets.get(i - 1) + 1;
            long actual = offsets.get(i);
            
            // Note: In a busy topic, offsets might not be perfectly sequential
            // But for a test topic, they should be
            assertTrue(actual >= expected, 
                      "Offsets should be sequential or increasing. " +
                      "Expected >= " + expected + ", got " + actual);
        }
        
        logger.info("✓ Ordering preserved in partition {}", partition);
        logger.info("  Offsets: {}", offsets);
    }
    
    @Test
    @Order(5)
    @DisplayName("Consistent hashing - same key always goes to same partition")
    void testConsistentHashing() {
        logger.info("Testing consistent hashing");
        
        Properties props = PartitioningHelper.getBaseConfig();
        
        String key = "consistent-test";
        Set<Integer> partitions = new HashSet<>();
        
        // Send messages multiple times and verify they always go to the same partition
        for (int round = 0; round < 3; round++) {
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(TOPIC_NAME, key, "round-" + round);
            
            try {
                RecordMetadata metadata = producer.send(record).get();
                partitions.add(metadata.partition());
                
                logger.debug("Round {}: message sent to partition {}", 
                           round, metadata.partition());
            } catch (Exception e) {
                fail("Failed to send message: " + e.getMessage());
            }
            
            producer.close();
        }
        
        // All rounds should use the same partition
        assertEquals(1, partitions.size(), 
                    "Same key should always hash to same partition across different producers");
        
        logger.info("✓ Consistent hashing verified - partition {}", 
                   partitions.iterator().next());
    }
    
    @Test
    @Order(6)
    @DisplayName("Multiple keys with same suffix have different partitions")
    void testKeyDistribution() {
        logger.info("Testing key distribution patterns");
        
        Properties props = PartitioningHelper.getBaseConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // Create keys with similar patterns
        Map<String, Integer> keyPartitions = new HashMap<>();
        
        for (int i = 0; i < 10; i++) {
            String key = "user-" + i;
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(TOPIC_NAME, key, "value");
            
            try {
                RecordMetadata metadata = producer.send(record).get();
                keyPartitions.put(key, metadata.partition());
            } catch (Exception e) {
                fail("Failed to send message: " + e.getMessage());
            }
        }
        
        producer.close();
        
        // Verify distribution
        Set<Integer> usedPartitions = new HashSet<>(keyPartitions.values());
        
        logger.info("✓ 10 sequential keys distributed across {} partitions", 
                   usedPartitions.size());
        
        // Good distribution should use multiple partitions
        assertTrue(usedPartitions.size() >= 3, 
                  "Sequential keys should be well distributed. Used: " + usedPartitions.size());
    }
}
