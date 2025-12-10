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
 * Tests demonstrating hot partition problems and solutions.
 * 
 * Hot partition occurs when:
 * - One key gets much more traffic than others
 * - All messages for that key go to one partition
 * - That partition becomes overloaded
 * 
 * Solution: Use composite keys to distribute load
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("hot-partition")
class HotPartitionTest {
    
    private static final Logger logger = LoggerFactory.getLogger(HotPartitionTest.class);
    private static final String TOPIC_NAME = "hot-partition-test";
    
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
    @DisplayName("Demonstrate hot partition problem")
    void testHotPartitionProblem() {
        logger.info("Demonstrating hot partition problem");
        
        Properties props = PartitioningHelper.getBaseConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        Map<String, Integer> keyCounts = new HashMap<>();
        
        // Simulate skewed load - one "celebrity" user gets 70% of traffic
        String celebrityKey = "celebrity-user-1234";
        String[] regularKeys = {"user-A", "user-B", "user-C", "user-D", "user-E"};
        
        // Send 70 messages for celebrity
        for (int i = 0; i < 70; i++) {
            keyCounts.put(celebrityKey, keyCounts.getOrDefault(celebrityKey, 0) + 1);
            
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(TOPIC_NAME, celebrityKey, "message-" + i);
            
            try {
                producer.send(record).get();
            } catch (Exception e) {
                fail("Failed to send message: " + e.getMessage());
            }
        }
        
        // Send 6 messages each for regular users (30 total)
        for (String key : regularKeys) {
            for (int i = 0; i < 6; i++) {
                keyCounts.put(key, keyCounts.getOrDefault(key, 0) + 1);
                
                ProducerRecord<String, String> record = 
                    new ProducerRecord<>(TOPIC_NAME, key, "message-" + i);
                
                try {
                    producer.send(record).get();
                } catch (Exception e) {
                    fail("Failed to send message: " + e.getMessage());
                }
            }
        }
        
        producer.close();
        
        // Analyze distribution
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        
        PartitionDistributionAnalyzer analyzer = 
            new PartitionDistributionAnalyzer(bootstrapServers);
        PartitionDistributionAnalyzer.DistributionReport report = 
            analyzer.analyzeDistribution(TOPIC_NAME);
        
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Hot Partition Problem Demonstration                      ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        System.out.println("Message Distribution:");
        keyCounts.entrySet().stream()
            .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
            .forEach(entry -> 
                System.out.printf("  %-25s: %3d messages (%.1f%%)\n",
                                 entry.getKey(),
                                 entry.getValue(),
                                 entry.getValue() * 100.0 / 100));
        
        report.print();
        
        // Verify hot partition exists
        assertTrue(report.hasHotPartitions(1.5), 
                  "Hot partition should be detected (>1.5x average)");
        
        PartitionInfo maxPartition = report.getMaxPartition();
        assertNotNull(maxPartition);
        assertTrue(maxPartition.getMessageCount() >= 70, 
                  "Max partition should have at least 70 messages (celebrity traffic)");
        
        logger.info("✓ Hot partition problem demonstrated");
        logger.info("  Hottest partition has {} messages ({:.1f}%)",
                   maxPartition.getMessageCount(),
                   maxPartition.getPercentage(report.getTotalMessages()));
    }
    
    @Test
    @Order(2)
    @DisplayName("Solve hot partition with composite keys")
    void testHotPartitionSolution() {
        logger.info("Testing hot partition solution with composite keys");
        
        Properties props = PartitioningHelper.getCompositeKeyPartitionerConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        String celebrityId = "celebrity-user-1234";
        String[] regularKeys = {"user-A", "user-B", "user-C", "user-D", "user-E"};
        
        // Send 70 messages for celebrity using COMPOSITE KEYS
        for (int i = 0; i < 70; i++) {
            // Add suffix to distribute load
            String compositeKey = PartitioningHelper.generateCompositeKeyWithSequence(
                celebrityId, i);
            
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(TOPIC_NAME, compositeKey, "message-" + i);
            
            try {
                producer.send(record).get();
            } catch (Exception e) {
                fail("Failed to send message: " + e.getMessage());
            }
        }
        
        // Send regular traffic
        for (String key : regularKeys) {
            for (int i = 0; i < 6; i++) {
                ProducerRecord<String, String> record = 
                    new ProducerRecord<>(TOPIC_NAME, key, "message-" + i);
                
                try {
                    producer.send(record).get();
                } catch (Exception e) {
                    fail("Failed to send message: " + e.getMessage());
                }
            }
        }
        
        producer.close();
        
        // Analyze distribution
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        
        PartitionDistributionAnalyzer analyzer = 
            new PartitionDistributionAnalyzer(bootstrapServers);
        PartitionDistributionAnalyzer.DistributionReport report = 
            analyzer.analyzeDistribution(TOPIC_NAME);
        
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Hot Partition Solution: Composite Keys                   ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        System.out.println("Celebrity traffic now distributed using composite keys:");
        System.out.println("  Original key: " + celebrityId);
        System.out.println("  Composite keys: " + celebrityId + "-00000000, " + 
                         celebrityId + "-00000001, etc.");
        
        report.print();
        
        // Verify better distribution
        double stdDev = report.getStandardDeviation();
        long avgPerPartition = report.getAverageMessagesPerPartition();
        double coefficientOfVariation = stdDev / avgPerPartition;
        
        logger.info("✓ Distribution metrics:");
        logger.info("  Standard deviation: {:.2f}", stdDev);
        logger.info("  Average per partition: {}", avgPerPartition);
        logger.info("  Coefficient of variation: {:.2f}", coefficientOfVariation);
        
        // Better distribution should have lower coefficient of variation
        // This is a rough heuristic - adjust based on your data
        assertTrue(coefficientOfVariation < 0.5, 
                  "Composite keys should improve distribution (CV < 0.5)");
    }
    
    @Test
    @Order(3)
    @DisplayName("Compare hot partition vs composite key distribution")
    void testCompareDistributions() {
        logger.info("Comparing distributions");
        
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        
        // Create two topics for comparison
        String hotTopic = "hot-partition-demo";
        String compositeTopic = "composite-key-demo";
        
        createTopic(hotTopic);
        createTopic(compositeTopic);
        
        String celebrityId = "celebrity-123";
        int celebrityMessages = 100;
        int regularMessages = 20;
        
        // Topic 1: Hot partition (single key)
        Properties props1 = PartitioningHelper.getDefaultPartitionerConfig();
        KafkaProducer<String, String> producer1 = new KafkaProducer<>(props1);
        
        for (int i = 0; i < celebrityMessages; i++) {
            producer1.send(new ProducerRecord<>(hotTopic, celebrityId, "msg-" + i));
        }
        
        for (int i = 0; i < regularMessages; i++) {
            producer1.send(new ProducerRecord<>(hotTopic, "regular-" + i, "msg-" + i));
        }
        
        producer1.close();
        
        // Topic 2: Composite keys
        Properties props2 = PartitioningHelper.getCompositeKeyPartitionerConfig();
        KafkaProducer<String, String> producer2 = new KafkaProducer<>(props2);
        
        for (int i = 0; i < celebrityMessages; i++) {
            String compositeKey = PartitioningHelper.generateCompositeKeyWithSequence(celebrityId, i);
            producer2.send(new ProducerRecord<>(compositeTopic, compositeKey, "msg-" + i));
        }
        
        for (int i = 0; i < regularMessages; i++) {
            producer2.send(new ProducerRecord<>(compositeTopic, "regular-" + i, "msg-" + i));
        }
        
        producer2.close();
        
        // Analyze both
        PartitionDistributionAnalyzer analyzer = new PartitionDistributionAnalyzer(bootstrapServers);
        
        PartitionDistributionAnalyzer.DistributionReport report1 = 
            analyzer.analyzeDistribution(hotTopic);
        PartitionDistributionAnalyzer.DistributionReport report2 = 
            analyzer.analyzeDistribution(compositeTopic);
        
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Distribution Comparison                                  ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        System.out.println("Approach 1: Single Key (Hot Partition)");
        report1.printSummary();
        
        System.out.println("\nApproach 2: Composite Keys (Distributed)");
        report2.printSummary();
        
        System.out.println("\nImprovement:");
        System.out.printf("  Standard Deviation: %.2f → %.2f (%.1f%% reduction)\n",
                         report1.getStandardDeviation(),
                         report2.getStandardDeviation(),
                         (1 - report2.getStandardDeviation() / report1.getStandardDeviation()) * 100);
        
        // Composite keys should have lower standard deviation
        assertTrue(report2.getStandardDeviation() < report1.getStandardDeviation(),
                  "Composite keys should improve distribution");
        
        logger.info("✓ Composite keys show improved distribution");
    }
    
    private void createTopic(String topicName) {
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic topic = new NewTopic(topicName, 6, (short) 1);
            admin.createTopics(Collections.singletonList(topic)).all().get(10, TimeUnit.SECONDS);
            logger.info("Topic created: {}", topicName);
        } catch (Exception e) {
            logger.info("Topic already exists: {}", topicName);
        }
    }
}
