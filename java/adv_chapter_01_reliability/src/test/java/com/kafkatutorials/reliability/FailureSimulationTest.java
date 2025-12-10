package com.kafkatutorials.reliability;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Failure simulation tests for Kafka producers.
 * Mirrors the bash simulate_failure.sh script functionality.
 */
@Tag("failure-simulation")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FailureSimulationTest {
    
    private static final String TEST_TOPIC = "failure-test-topic-java";
    
    private AdminClient adminClient;
    
    @BeforeEach
    void setUp() throws Exception {
        Properties adminProps = ProducerConfigHelper.getBaseProducerConfig();
        adminClient = AdminClient.create(adminProps);
        
        // Create test topic
        NewTopic topic = new NewTopic(TEST_TOPIC, 1, (short) 1);
        
        try {
            adminClient.createTopics(Collections.singleton(topic)).all().get();
            System.out.println("[Setup] ✓ Topic created: " + TEST_TOPIC);
        } catch (Exception e) {
            System.out.println("[Setup] Topic already exists or creation skipped");
        }
        
        Thread.sleep(1000);
    }
    
    @AfterEach
    void tearDown() throws Exception {
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
    @DisplayName("Scenario 1: Short timeout simulation")
    void testShortTimeout() {
        System.out.println("\n[Scenario 1] Short timeout - simulating request timeout...");
        System.out.println("Configuration: request.timeout.ms=1000 (very short)");
        
        Properties props = ProducerConfigHelper.getBaseProducerConfig();
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);  // Very short
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 2);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-timeout");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            boolean timeoutOccurred = false;
            
            for (int i = 1; i <= 2; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TEST_TOPIC,
                    "timeout-test-message-" + i
                );
                
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.println("  Message " + i + " sent successfully (offset: " + metadata.offset() + ")");
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TimeoutException) {
                        timeoutOccurred = true;
                        System.out.println("  ⚠ Message " + i + " timed out (expected with short timeout)");
                    } else {
                        System.err.println("  ✗ Unexpected error: " + e.getCause().getMessage());
                    }
                } catch (Exception e) {
                    System.err.println("  ✗ Error sending message: " + e.getMessage());
                }
            }
            
            System.out.println("[Scenario 1] Complete - observe any timeout warnings above");
            if (timeoutOccurred) {
                System.out.println("             Note: Timeouts are expected with very short timeout settings");
            }
        }
    }
    
    @Test
    @Order(2)
    @DisplayName("Scenario 2: Aggressive retries with backoff")
    void testAggressiveRetries() throws Exception {
        System.out.println("\n[Scenario 2] Aggressive retries with backoff...");
        System.out.println("Configuration: retries=5, retry.backoff.ms=500");
        
        Properties props = ProducerConfigHelper.getBaseProducerConfig();
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 500);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-aggressive-retry");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int successCount = 0;
            
            for (int i = 1; i <= 3; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TEST_TOPIC,
                    "retry-message-" + i
                );
                
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    successCount++;
                    System.out.println("  Message " + i + " sent (offset: " + metadata.offset() + ")");
                } catch (Exception e) {
                    System.err.println("  ✗ Message " + i + " failed after retries: " + e.getMessage());
                }
            }
            
            assertTrue(successCount > 0, "At least some messages should succeed with retries");
            System.out.println("[Scenario 2] ✓ Messages sent with retry protection");
            System.out.println("             Successful: " + successCount + "/3");
        }
    }
    
    @Test
    @Order(3)
    @DisplayName("Scenario 3: Fire-and-forget mode (acks=0)")
    void testFireAndForget() throws Exception {
        System.out.println("\n[Scenario 3] Testing acks=0 (fire-and-forget, no guarantees)...");
        System.out.println("⚠ WARNING: acks=0 provides NO delivery guarantees!");
        
        Properties props = ProducerConfigHelper.getFireAndForgetConfig();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-no-ack");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int sentCount = 0;
            
            for (int i = 1; i <= 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    TEST_TOPIC,
                    "fire-and-forget-" + i
                );
                
                // With acks=0, send() returns immediately without waiting
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("  ✗ Error (may be lost): " + exception.getMessage());
                    }
                });
                sentCount++;
            }
            
            // Force flush to try to send buffered messages
            producer.flush();
            
            System.out.println("[Scenario 3] ⚠ Sent " + sentCount + " messages with acks=0");
            System.out.println("             Note: Some messages may be lost in production!");
            System.out.println("             This mode is NOT recommended for critical data.");
        }
    }
    
    @Test
    @Order(4)
    @DisplayName("Scenario 4: Compare acks levels")
    void testCompareAcksLevels() throws Exception {
        System.out.println("\n[Scenario 4] Comparing different acks levels...");
        
        Map<String, Long> results = new HashMap<>();
        
        // Test acks=0
        Properties props0 = ProducerConfigHelper.getBaseProducerConfig();
        props0.put(ProducerConfig.ACKS_CONFIG, "0");
        long start = System.currentTimeMillis();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props0)) {
            producer.send(new ProducerRecord<>(TEST_TOPIC, "acks-0-test"));
            producer.flush();
        }
        results.put("acks=0", System.currentTimeMillis() - start);
        
        // Test acks=1
        Properties props1 = ProducerConfigHelper.getBaseProducerConfig();
        props1.put(ProducerConfig.ACKS_CONFIG, "1");
        start = System.currentTimeMillis();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props1)) {
            producer.send(new ProducerRecord<>(TEST_TOPIC, "acks-1-test")).get();
        }
        results.put("acks=1", System.currentTimeMillis() - start);
        
        // Test acks=all
        Properties propsAll = ProducerConfigHelper.getBaseProducerConfig();
        propsAll.put(ProducerConfig.ACKS_CONFIG, "all");
        start = System.currentTimeMillis();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(propsAll)) {
            producer.send(new ProducerRecord<>(TEST_TOPIC, "acks-all-test")).get();
        }
        results.put("acks=all", System.currentTimeMillis() - start);
        
        System.out.println("[Scenario 4] ✓ Performance comparison (approximate):");
        results.forEach((key, value) -> 
            System.out.println("             " + key + ": " + value + "ms"));
        System.out.println("             Note: acks=0 is fastest but least reliable");
        System.out.println("                   acks=all is slowest but most reliable");
    }
}

