package com.kafkatutorials.performance;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance benchmark tests for Kafka producers.
 * 
 * These tests compare different performance profiles:
 * - High throughput vs low latency
 * - Different batch sizes
 * - Different linger times
 * 
 * Run with: ./gradlew runPerformanceTests
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("performance")
class PerformanceBenchmarkTest {
    
    private static final Logger logger = LoggerFactory.getLogger(PerformanceBenchmarkTest.class);
    private static final String TOPIC_NAME = "perf-test-topic";
    private static final int MESSAGE_COUNT = 5000;
    private static final int MESSAGE_SIZE = 1024; // 1KB
    
    private ThroughputBenchmark benchmark;
    
    @BeforeEach
    void setUp() {
        benchmark = new ThroughputBenchmark();
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
    @DisplayName("Test High Throughput Configuration")
    void testHighThroughputConfig() {
        logger.info("Testing high throughput configuration");
        
        Properties config = PerformanceConfigHelper.getHighThroughputConfig();
        config.setProperty("client.id", "high-throughput-test");
        
        BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
        
        assertNotNull(result);
        assertEquals(MESSAGE_COUNT, result.getSuccessCount(), "All messages should be sent successfully");
        assertEquals(0, result.getFailureCount(), "No failures should occur");
        assertTrue(result.getThroughputMessagesPerSec() > 0, "Throughput should be positive");
        
        result.print();
        
        logger.info("High throughput test completed: {} msg/sec", 
                   String.format("%.2f", result.getThroughputMessagesPerSec()));
    }
    
    @Test
    @Order(2)
    @DisplayName("Test Low Latency Configuration")
    void testLowLatencyConfig() {
        logger.info("Testing low latency configuration");
        
        Properties config = PerformanceConfigHelper.getLowLatencyConfig();
        config.setProperty("client.id", "low-latency-test");
        
        BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
        
        assertNotNull(result);
        assertEquals(MESSAGE_COUNT, result.getSuccessCount(), "All messages should be sent successfully");
        assertTrue(result.getP95LatencyMs() < 100, "P95 latency should be low");
        
        result.print();
        
        logger.info("Low latency test completed: {} ms p95", result.getP95LatencyMs());
    }
    
    @Test
    @Order(3)
    @DisplayName("Test Balanced Configuration")
    void testBalancedConfig() {
        logger.info("Testing balanced configuration");
        
        Properties config = PerformanceConfigHelper.getBalancedConfig();
        config.setProperty("client.id", "balanced-test");
        
        BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
        
        assertNotNull(result);
        assertEquals(MESSAGE_COUNT, result.getSuccessCount(), "All messages should be sent successfully");
        assertTrue(result.getThroughputMessagesPerSec() > 100, "Should have reasonable throughput");
        assertTrue(result.getP95LatencyMs() < 500, "Should have reasonable latency");
        
        result.print();
        
        logger.info("Balanced test completed");
    }
    
    @Test
    @Order(4)
    @DisplayName("Compare Throughput vs Latency Profiles")
    void testCompareProfiles() {
        logger.info("Comparing throughput vs latency profiles");
        
        Properties highThroughput = PerformanceConfigHelper.getHighThroughputConfig();
        highThroughput.setProperty("client.id", "profile-high-throughput");
        
        Properties lowLatency = PerformanceConfigHelper.getLowLatencyConfig();
        lowLatency.setProperty("client.id", "profile-low-latency");
        
        Properties balanced = PerformanceConfigHelper.getBalancedConfig();
        balanced.setProperty("client.id", "profile-balanced");
        
        List<BenchmarkResult> results = benchmark.compareConfigs(
            TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE,
            highThroughput, lowLatency, balanced
        );
        
        assertEquals(3, results.size(), "Should have 3 results");
        
        ThroughputBenchmark.printComparisonTable(results);
        
        // Verify high throughput config has highest throughput
        BenchmarkResult highThroughputResult = results.get(0);
        BenchmarkResult lowLatencyResult = results.get(1);
        
        assertTrue(highThroughputResult.getThroughputMessagesPerSec() > 
                   lowLatencyResult.getThroughputMessagesPerSec(),
                   "High throughput config should have higher throughput than low latency");
        
        logger.info("Profile comparison completed");
    }
    
    @Test
    @Order(5)
    @DisplayName("Test Different Batch Sizes")
    void testBatchSizes() {
        logger.info("Testing different batch sizes");
        
        int[] batchSizes = {1024, 16384, 65536}; // 1KB, 16KB, 64KB
        
        for (int batchSize : batchSizes) {
            Properties config = PerformanceConfigHelper.getBatchSizeConfig(batchSize);
            config.setProperty("client.id", "batch-" + batchSize);
            
            BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
            
            assertNotNull(result);
            assertEquals(MESSAGE_COUNT, result.getSuccessCount());
            
            logger.info("Batch size {} bytes: {} msg/sec", 
                       batchSize, String.format("%.2f", result.getThroughputMessagesPerSec()));
        }
    }
    
    @Test
    @Order(6)
    @DisplayName("Test Different Linger Times")
    void testLingerTimes() {
        logger.info("Testing different linger times");
        
        int[] lingerMs = {0, 5, 10, 50}; // 0ms to 50ms
        
        for (int linger : lingerMs) {
            Properties config = PerformanceConfigHelper.getLingerConfig(linger);
            config.setProperty("client.id", "linger-" + linger);
            
            BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
            
            assertNotNull(result);
            assertEquals(MESSAGE_COUNT, result.getSuccessCount());
            
            logger.info("Linger {} ms: {} msg/sec, {} ms p95", 
                       linger, 
                       String.format("%.2f", result.getThroughputMessagesPerSec()),
                       result.getP95LatencyMs());
        }
    }
    
    @Test
    @Order(7)
    @DisplayName("Test Message Size Impact")
    void testMessageSizes() {
        logger.info("Testing different message sizes");
        
        Properties config = PerformanceConfigHelper.getBalancedConfig();
        int[] messageSizes = {100, 1024, 10240}; // 100B, 1KB, 10KB
        
        for (int messageSize : messageSizes) {
            config.setProperty("client.id", "msgsize-" + messageSize);
            
            BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, 1000, messageSize);
            
            assertNotNull(result);
            assertTrue(result.getSuccessCount() > 0);
            
            logger.info("Message size {} bytes: {} msg/sec, {} MB/sec", 
                       messageSize,
                       String.format("%.2f", result.getThroughputMessagesPerSec()),
                       String.format("%.2f", result.getThroughputMBPerSec()));
        }
    }
    
    @Test
    @Order(8)
    @DisplayName("Verify Batching Efficiency")
    void testBatchingEfficiency() {
        logger.info("Verifying batching efficiency");
        
        // No batching (tiny batch, no linger)
        Properties noBatch = PerformanceConfigHelper.getLowLatencyConfig();
        noBatch.setProperty("client.id", "no-batching");
        
        BenchmarkResult noBatchResult = benchmark.runBenchmark(noBatch, TOPIC_NAME, 2000, MESSAGE_SIZE);
        
        // With batching (large batch, linger)
        Properties withBatch = PerformanceConfigHelper.getHighThroughputConfig();
        withBatch.setProperty("client.id", "with-batching");
        
        BenchmarkResult withBatchResult = benchmark.runBenchmark(withBatch, TOPIC_NAME, 2000, MESSAGE_SIZE);
        
        // Batching should improve throughput
        double improvement = withBatchResult.getThroughputMessagesPerSec() / 
                           noBatchResult.getThroughputMessagesPerSec();
        
        logger.info("Batching improvement: {}x", String.format("%.2f", improvement));
        
        assertTrue(improvement > 1.2, 
                  "Batching should improve throughput by at least 20%");
        
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Batching Efficiency Comparison                           ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        System.out.printf("Without batching: %,.2f msg/sec\n", noBatchResult.getThroughputMessagesPerSec());
        System.out.printf("With batching:    %,.2f msg/sec\n", withBatchResult.getThroughputMessagesPerSec());
        System.out.printf("Improvement:      %.2fx\n\n", improvement);
    }
}
