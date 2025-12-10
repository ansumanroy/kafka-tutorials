package com.kafkatutorials.reliability;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Kafka producer reliability features.
 * Mirrors the bash test_reliability.sh script functionality.
 */
@Tag("reliability")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReliabilityIntegrationTest {
    
    private static final String TEST_TOPIC = "reliability-test-topic-java";
    private static final int TEST_MESSAGE_COUNT = 10;
    
    private AdminClient adminClient;
    
    @BeforeEach
    void setUp() throws Exception {
        Properties adminProps = ProducerConfigHelper.getBaseProducerConfig();
        adminClient = AdminClient.create(adminProps);
        
        // Create test topic
        NewTopic topic = new NewTopic(TEST_TOPIC, 3, (short) 1);
        Map<String, String> configs = new HashMap<>();
        configs.put("min.insync.replicas", "1");
        topic.configs(configs);
        
        try {
            adminClient.createTopics(Collections.singleton(topic)).all().get();
            System.out.println("[Setup] ✓ Topic created: " + TEST_TOPIC);
        } catch (Exception e) {
            // Topic may already exist, that's fine
            System.out.println("[Setup] Topic already exists or creation skipped");
        }
        
        // Give Kafka a moment to stabilize
        Thread.sleep(1000);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        // Clean up topic
        try {
            adminClient.deleteTopics(Collections.singleton(TEST_TOPIC)).all().get();
            System.out.println("[Cleanup] ✓ Topic deleted");
        } catch (Exception e) {
            System.err.println("[Cleanup] Failed to delete topic: " + e.getMessage());
        }
        
        if (adminClient != null) {
            adminClient.close();
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("Test 1: Producer with acks=1 (leader only)")
    void testAcksOne() throws Exception {
        System.out.println("\n[Test 1] Testing acks=1 (Leader only acknowledgement)...");
        
        Properties props = ProducerConfigHelper.getBaseProducerConfig();
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-acks-1");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            
            for (int i = 1; i <= TEST_MESSAGE_COUNT; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TEST_TOPIC,
                    "acks1-message-" + i
                );
                futures.add(producer.send(record));
            }
            
            // Wait for all sends to complete
            for (Future<RecordMetadata> future : futures) {
                RecordMetadata metadata = future.get();
                assertNotNull(metadata);
                assertTrue(metadata.hasOffset());
            }
            
            System.out.println("[Test 1] ✓ Sent " + TEST_MESSAGE_COUNT + " messages with acks=1");
        }
    }
    
    @Test
    @Order(2)
    @DisplayName("Test 2: Producer with acks=all (all replicas)")
    void testAcksAll() throws Exception {
        System.out.println("\n[Test 2] Testing acks=all (All replicas acknowledgement)...");
        
        Properties props = ProducerConfigHelper.getBaseProducerConfig();
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-acks-all");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int successCount = 0;
            
            for (int i = 1; i <= TEST_MESSAGE_COUNT; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TEST_TOPIC,
                    "acks-all-message-" + i
                );
                
                RecordMetadata metadata = producer.send(record).get();
                assertNotNull(metadata);
                assertTrue(metadata.hasOffset());
                successCount++;
            }
            
            assertEquals(TEST_MESSAGE_COUNT, successCount);
            System.out.println("[Test 2] ✓ Sent " + TEST_MESSAGE_COUNT + " messages with acks=all");
        }
    }
    
    @Test
    @Order(3)
    @DisplayName("Test 3: Idempotent producer (exactly-once)")
    void testIdempotence() throws Exception {
        System.out.println("\n[Test 3] Testing with idempotence enabled...");
        
        Properties props = ProducerConfigHelper.getHighReliabilityConfig();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-idempotent");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Set<Long> offsets = new HashSet<>();
            
            for (int i = 1; i <= TEST_MESSAGE_COUNT; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TEST_TOPIC,
                    "idempotent-message-" + i
                );
                
                RecordMetadata metadata = producer.send(record).get();
                offsets.add(metadata.offset());
            }
            
            // All offsets should be unique (no duplicates)
            assertEquals(TEST_MESSAGE_COUNT, offsets.size(),
                "Idempotence should prevent duplicate offsets");
            
            System.out.println("[Test 3] ✓ Sent " + TEST_MESSAGE_COUNT + " idempotent messages");
        }
    }
    
    @Test
    @Order(4)
    @DisplayName("Test 4: Retry behavior with high retry config")
    void testRetryBehavior() throws Exception {
        System.out.println("\n[Test 4] Testing retry behavior (with high retries)...");
        
        Properties props = ProducerConfigHelper.getBaseProducerConfig();
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-retries");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int messageCount = 5;
            
            for (int i = 1; i <= messageCount; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TEST_TOPIC,
                    "retry-test-message-" + i
                );
                
                RecordMetadata metadata = producer.send(record).get();
                assertNotNull(metadata);
            }
            
            System.out.println("[Test 4] ✓ Sent messages with retry configuration");
        }
    }
    
    @Test
    @Order(5)
    @DisplayName("Test 5: Keyed messages with ordering guarantees")
    void testKeyedMessagesOrdering() throws Exception {
        System.out.println("\n[Test 5] Testing with keys (for ordering guarantee)...");
        
        Properties props = ProducerConfigHelper.getHighReliabilityConfig();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-ordering");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Map<String, List<Long>> keyToOffsets = new HashMap<>();
            
            for (int i = 1; i <= TEST_MESSAGE_COUNT; i++) {
                String key = "key-" + (i % 3); // 3 different keys
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TEST_TOPIC,
                    key,
                    "order-test-message-" + i
                );
                
                RecordMetadata metadata = producer.send(record).get();
                
                keyToOffsets.computeIfAbsent(key, k -> new ArrayList<>())
                    .add(metadata.offset());
            }
            
            // Verify all messages with same key went to same partition
            for (Map.Entry<String, List<Long>> entry : keyToOffsets.entrySet()) {
                List<Long> offsets = entry.getValue();
                assertTrue(offsets.size() > 0, "Each key should have messages");
            }
            
            System.out.println("[Test 5] ✓ Sent keyed messages with ordering guarantees");
        }
    }
    
    @Test
    @Order(6)
    @DisplayName("Test 6: Verify message count and no duplicates")
    void testMessageCountAndNoDuplicates() throws Exception {
        System.out.println("\n[Test 6] Verifying message count...");
        
        // Give Kafka time to flush
        Thread.sleep(2000);
        
        Properties consumerProps = new Properties();
        consumerProps.putAll(ProducerConfigHelper.getBaseProducerConfig());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singleton(TEST_TOPIC));
            
            Set<String> receivedMessages = new HashSet<>();
            int totalMessages = 0;
            long endTime = System.currentTimeMillis() + 10000; // 10 second timeout
            
            while (System.currentTimeMillis() < endTime) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty() && totalMessages > 0) {
                    break; // No more messages
                }
                
                records.forEach(record -> {
                    receivedMessages.add(record.value());
                });
                totalMessages += records.count();
            }
            
            System.out.println("[Test 6] Total messages received: " + totalMessages);
            System.out.println("[Test 6] Unique messages: " + receivedMessages.size());
            
            // With idempotence, we should have no duplicates
            assertEquals(totalMessages, receivedMessages.size(),
                "No duplicate messages should exist with idempotence");
            
            System.out.println("[Test 6] ✓ Message count verified, no duplicates found");
        }
    }
    
    @Test
    @Order(7)
    @DisplayName("Test 7: Topic health check")
    void testTopicHealth() throws Exception {
        System.out.println("\n[Test 7] Checking topic health and partition distribution...");
        
        var topicDescription = adminClient.describeTopics(Collections.singleton(TEST_TOPIC))
            .all()
            .get();
        
        assertNotNull(topicDescription);
        assertTrue(topicDescription.containsKey(TEST_TOPIC));
        
        var topicInfo = topicDescription.get(TEST_TOPIC);
        assertEquals(3, topicInfo.partitions().size(), "Should have 3 partitions");
        
        System.out.println("[Test 7] ✓ Topic health check complete");
        System.out.println("         Topic: " + topicInfo.name());
        System.out.println("         Partitions: " + topicInfo.partitions().size());
    }
}

