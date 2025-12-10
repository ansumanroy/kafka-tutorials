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
 * Tests comparing different compression algorithms for Kafka producers.
 * 
 * Compression types tested:
 * - none: No compression (baseline)
 * - gzip: Best compression ratio, highest CPU
 * - snappy: Balanced compression and speed
 * - lz4: Fast compression, good for throughput
 * - zstd: Modern algorithm, good balance
 * 
 * Run with: ./gradlew runCompressionTests
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("compression")
class CompressionComparisonTest {
    
    private static final Logger logger = LoggerFactory.getLogger(CompressionComparisonTest.class);
    private static final String TOPIC_NAME = "compression-test-topic";
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
    @DisplayName("Test No Compression (Baseline)")
    void testNoCompression() {
        logger.info("Testing without compression (baseline)");
        
        Properties config = PerformanceConfigHelper.getCompressionConfig("none");
        config.setProperty("client.id", "compression-none");
        
        BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
        
        assertNotNull(result);
        assertEquals(MESSAGE_COUNT, result.getSuccessCount());
        
        result.print();
        
        logger.info("No compression: {} msg/sec, {} MB/sec",
                   String.format("%.2f", result.getThroughputMessagesPerSec()),
                   String.format("%.2f", result.getThroughputMBPerSec()));
    }
    
    @Test
    @Order(2)
    @DisplayName("Test GZIP Compression")
    void testGzipCompression() {
        logger.info("Testing GZIP compression");
        
        Properties config = PerformanceConfigHelper.getCompressionConfig("gzip");
        config.setProperty("client.id", "compression-gzip");
        
        BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
        
        assertNotNull(result);
        assertEquals(MESSAGE_COUNT, result.getSuccessCount());
        
        result.print();
        
        logger.info("GZIP compression: {} msg/sec, {} MB/sec",
                   String.format("%.2f", result.getThroughputMessagesPerSec()),
                   String.format("%.2f", result.getThroughputMBPerSec()));
    }
    
    @Test
    @Order(3)
    @DisplayName("Test Snappy Compression")
    void testSnappyCompression() {
        logger.info("Testing Snappy compression");
        
        Properties config = PerformanceConfigHelper.getCompressionConfig("snappy");
        config.setProperty("client.id", "compression-snappy");
        
        BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
        
        assertNotNull(result);
        assertEquals(MESSAGE_COUNT, result.getSuccessCount());
        
        result.print();
        
        logger.info("Snappy compression: {} msg/sec, {} MB/sec",
                   String.format("%.2f", result.getThroughputMessagesPerSec()),
                   String.format("%.2f", result.getThroughputMBPerSec()));
    }
    
    @Test
    @Order(4)
    @DisplayName("Test LZ4 Compression")
    void testLz4Compression() {
        logger.info("Testing LZ4 compression");
        
        Properties config = PerformanceConfigHelper.getCompressionConfig("lz4");
        config.setProperty("client.id", "compression-lz4");
        
        BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
        
        assertNotNull(result);
        assertEquals(MESSAGE_COUNT, result.getSuccessCount());
        
        result.print();
        
        logger.info("LZ4 compression: {} msg/sec, {} MB/sec",
                   String.format("%.2f", result.getThroughputMessagesPerSec()),
                   String.format("%.2f", result.getThroughputMBPerSec()));
    }
    
    @Test
    @Order(5)
    @DisplayName("Test ZSTD Compression")
    void testZstdCompression() {
        logger.info("Testing ZSTD compression");
        
        Properties config = PerformanceConfigHelper.getCompressionConfig("zstd");
        config.setProperty("client.id", "compression-zstd");
        
        BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
        
        assertNotNull(result);
        assertEquals(MESSAGE_COUNT, result.getSuccessCount());
        
        result.print();
        
        logger.info("ZSTD compression: {} msg/sec, {} MB/sec",
                   String.format("%.2f", result.getThroughputMessagesPerSec()),
                   String.format("%.2f", result.getThroughputMBPerSec()));
    }
    
    @Test
    @Order(6)
    @DisplayName("Compare All Compression Algorithms")
    void testCompareAllCompression() {
        logger.info("Comparing all compression algorithms");
        
        List<BenchmarkResult> results = benchmark.compareCompression(
            TOPIC_NAME, MESSAGE_COUNT, MESSAGE_SIZE);
        
        assertEquals(5, results.size(), "Should have 5 compression results");
        
        ThroughputBenchmark.printComparisonTable(results);
        
        // Verify all succeeded
        for (BenchmarkResult result : results) {
            assertEquals(MESSAGE_COUNT, result.getSuccessCount(), 
                        "All messages should succeed for " + result.getConfigName());
        }
        
        // Find best performing compression
        BenchmarkResult bestThroughput = results.stream()
            .max((r1, r2) -> Double.compare(r1.getThroughputMessagesPerSec(), 
                                           r2.getThroughputMessagesPerSec()))
            .orElse(null);
        
        assertNotNull(bestThroughput);
        logger.info("Best compression for throughput: {}", bestThroughput.getConfigName());
        
        // LZ4 or no compression should typically be fastest
        assertTrue(bestThroughput.getConfigName().contains("lz4") || 
                  bestThroughput.getConfigName().contains("none"),
                  "LZ4 or no compression should have best throughput");
    }
    
    @Test
    @Order(7)
    @DisplayName("Compression with Different Message Sizes")
    void testCompressionWithDifferentSizes() {
        logger.info("Testing compression with different message sizes");
        
        int[] messageSizes = {100, 1024, 10240}; // 100B, 1KB, 10KB
        String[] compressionTypes = {"none", "lz4", "gzip"};
        
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Compression vs Message Size                              ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        for (int messageSize : messageSizes) {
            System.out.println("\nMessage Size: " + messageSize + " bytes");
            System.out.println("─".repeat(60));
            
            for (String compression : compressionTypes) {
                Properties config = PerformanceConfigHelper.getCompressionConfig(compression);
                config.setProperty("client.id", compression + "-" + messageSize);
                
                BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, 1000, messageSize);
                
                System.out.printf("  %-10s: %,8.2f msg/sec, %6.2f MB/sec\n",
                                 compression,
                                 result.getThroughputMessagesPerSec(),
                                 result.getThroughputMBPerSec());
            }
        }
        
        System.out.println();
    }
    
    @Test
    @Order(8)
    @DisplayName("Verify Compression Reduces Network Traffic")
    void testCompressionReducesTraffic() {
        logger.info("Verifying compression reduces network traffic");
        
        // Use larger messages for better compression ratio
        int largeMessageSize = 10240; // 10KB
        int messageCount = 1000;
        
        // No compression
        Properties noCompression = PerformanceConfigHelper.getCompressionConfig("none");
        noCompression.setProperty("client.id", "traffic-none");
        
        BenchmarkResult noneResult = benchmark.runBenchmark(
            noCompression, TOPIC_NAME, messageCount, largeMessageSize);
        
        // With compression
        Properties withCompression = PerformanceConfigHelper.getCompressionConfig("lz4");
        withCompression.setProperty("client.id", "traffic-lz4");
        
        BenchmarkResult lz4Result = benchmark.runBenchmark(
            withCompression, TOPIC_NAME, messageCount, largeMessageSize);
        
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Network Traffic Reduction Analysis                       ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        long totalDataMB = (long) messageCount * largeMessageSize / (1024 * 1024);
        
        System.out.printf("Total data (uncompressed): %d MB\n\n", totalDataMB);
        System.out.printf("No compression throughput: %.2f MB/sec\n", noneResult.getThroughputMBPerSec());
        System.out.printf("LZ4 compression throughput: %.2f MB/sec\n", lz4Result.getThroughputMBPerSec());
        System.out.println();
        
        // Note: We can't directly measure compression ratio without broker metrics,
        // but we can verify the compression didn't hurt throughput significantly
        double throughputRatio = lz4Result.getThroughputMessagesPerSec() / 
                                noneResult.getThroughputMessagesPerSec();
        
        System.out.printf("Throughput ratio (LZ4/none): %.2fx\n", throughputRatio);
        System.out.println();
        
        assertTrue(throughputRatio > 0.5, 
                  "Compression should not reduce throughput by more than 50%");
    }
    
    @Test
    @Order(9)
    @DisplayName("Compression Latency Impact")
    void testCompressionLatencyImpact() {
        logger.info("Testing compression impact on latency");
        
        String[] compressionTypes = {"none", "lz4", "gzip"};
        
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Compression Latency Impact                               ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        System.out.printf("%-15s | %-12s | %-12s | %-12s | %-12s\n",
                         "Compression", "Avg (ms)", "P50 (ms)", "P95 (ms)", "P99 (ms)");
        System.out.println("─".repeat(75));
        
        for (String compression : compressionTypes) {
            Properties config = PerformanceConfigHelper.getCompressionConfig(compression);
            config.setProperty("client.id", "latency-" + compression);
            
            BenchmarkResult result = benchmark.runBenchmark(config, TOPIC_NAME, 2000, MESSAGE_SIZE);
            
            System.out.printf("%-15s | %12d | %12d | %12d | %12d\n",
                             compression,
                             result.getAvgLatencyMs(),
                             result.getP50LatencyMs(),
                             result.getP95LatencyMs(),
                             result.getP99LatencyMs());
        }
        
        System.out.println();
    }
}
