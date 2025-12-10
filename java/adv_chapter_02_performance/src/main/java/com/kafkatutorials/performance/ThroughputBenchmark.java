package com.kafkatutorials.performance;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Throughput benchmark for Kafka producers.
 * 
 * This class provides methods to benchmark producer performance with different
 * configurations, message sizes, and compression settings.
 * 
 * Usage:
 *   ./gradlew runBenchmark
 *   
 * Or programmatically:
 *   ThroughputBenchmark benchmark = new ThroughputBenchmark();
 *   BenchmarkResult result = benchmark.runBenchmark(config, topic, messageCount, messageSize);
 */
public class ThroughputBenchmark {
    
    private static final Logger logger = LoggerFactory.getLogger(ThroughputBenchmark.class);
    
    /**
     * Run a throughput benchmark with the given configuration.
     * 
     * @param config Producer configuration
     * @param topicName Topic to produce to
     * @param messageCount Number of messages to send
     * @param messageSizeBytes Size of each message in bytes
     * @return BenchmarkResult containing performance metrics
     */
    public BenchmarkResult runBenchmark(Properties config, String topicName, 
                                       int messageCount, int messageSizeBytes) {
        
        String configName = config.getProperty("client.id", "benchmark");
        logger.info("Starting benchmark: {} - {} messages of {} bytes", 
                    configName, messageCount, messageSizeBytes);
        
        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);
        
        // Prepare message payload
        String messageValue = generateMessage(messageSizeBytes);
        
        // Track latencies and results
        List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);
        
        // Start timing
        long startTime = System.currentTimeMillis();
        
        // Send messages
        for (int i = 0; i < messageCount; i++) {
            String key = "key-" + i;
            long sendTime = System.currentTimeMillis();
            
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, messageValue);
            
            producer.send(record, (metadata, exception) -> {
                long latency = System.currentTimeMillis() - sendTime;
                
                if (exception == null) {
                    successCount.incrementAndGet();
                    latencies.add(latency);
                } else {
                    failureCount.incrementAndGet();
                    logger.error("Failed to send message: {}", exception.getMessage());
                }
                
                latch.countDown();
            });
        }
        
        // Wait for all callbacks
        try {
            boolean completed = latch.await(2, TimeUnit.MINUTES);
            if (!completed) {
                logger.warn("Benchmark did not complete within timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Benchmark interrupted", e);
        }
        
        // Flush and close
        producer.flush();
        long endTime = System.currentTimeMillis();
        producer.close();
        
        long duration = endTime - startTime;
        
        logger.info("Benchmark completed: {} ms", duration);
        
        return new BenchmarkResult(
            configName,
            messageCount,
            messageSizeBytes,
            duration,
            latencies,
            successCount.get(),
            failureCount.get()
        );
    }
    
    /**
     * Compare multiple configurations with the same workload.
     */
    public List<BenchmarkResult> compareConfigs(String topicName, int messageCount, 
                                               int messageSizeBytes, Properties... configs) {
        
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Running Configuration Comparison                         ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");
        System.out.printf("\nWorkload: %,d messages × %,d bytes = %,d KB total\n\n",
                         messageCount, messageSizeBytes, (messageCount * messageSizeBytes) / 1024);
        
        List<BenchmarkResult> results = new ArrayList<>();
        
        for (Properties config : configs) {
            String configName = config.getProperty("client.id", "unknown");
            PerformanceConfigHelper.printConfig(config, configName);
            
            BenchmarkResult result = runBenchmark(config, topicName, messageCount, messageSizeBytes);
            results.add(result);
            
            // Pause between tests
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        return results;
    }
    
    /**
     * Compare different compression algorithms.
     */
    public List<BenchmarkResult> compareCompression(String topicName, int messageCount, int messageSizeBytes) {
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Compression Algorithm Comparison                         ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        String[] compressionTypes = {"none", "gzip", "snappy", "lz4", "zstd"};
        List<Properties> configs = new ArrayList<>();
        
        for (String compression : compressionTypes) {
            Properties config = PerformanceConfigHelper.getCompressionConfig(compression);
            config.setProperty("client.id", "compression-" + compression);
            configs.add(config);
        }
        
        return compareConfigs(topicName, messageCount, messageSizeBytes, 
                            configs.toArray(new Properties[0]));
    }
    
    /**
     * Compare different batch sizes.
     */
    public List<BenchmarkResult> compareBatchSizes(String topicName, int messageCount, int messageSizeBytes) {
        System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Batch Size Comparison                                    ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝\n");
        
        int[] batchSizes = {1024, 8192, 16384, 32768, 65536}; // 1KB to 64KB
        List<Properties> configs = new ArrayList<>();
        
        for (int batchSize : batchSizes) {
            Properties config = PerformanceConfigHelper.getBatchSizeConfig(batchSize);
            config.setProperty("client.id", "batch-" + batchSize);
            configs.add(config);
        }
        
        return compareConfigs(topicName, messageCount, messageSizeBytes,
                            configs.toArray(new Properties[0]));
    }
    
    /**
     * Generate a message of specified size.
     */
    private String generateMessage(int sizeBytes) {
        StringBuilder sb = new StringBuilder(sizeBytes);
        String template = "abcdefghijklmnopqrstuvwxyz0123456789";
        
        while (sb.length() < sizeBytes) {
            sb.append(template);
        }
        
        return sb.substring(0, sizeBytes);
    }
    
    /**
     * Create topic if it doesn't exist.
     */
    public static void ensureTopicExists(String bootstrapServers, String topicName, 
                                        int partitions, short replicationFactor) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
            admin.createTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
            logger.info("Topic created: {}", topicName);
        } catch (Exception e) {
            // Topic might already exist
            logger.info("Topic already exists or creation failed: {}", topicName);
        }
    }
    
    /**
     * Print comparison table.
     */
    public static void printComparisonTable(List<BenchmarkResult> results) {
        System.out.println("\n╔═══════════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  Performance Comparison Summary                                                               ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════════════════════════════════════╝\n");
        
        System.out.printf("%-25s | %-15s | %-13s | %-12s | %-12s\n",
                         "Configuration", "Throughput", "Bandwidth", "P95 Latency", "Avg Latency");
        System.out.println("─".repeat(100));
        
        for (BenchmarkResult result : results) {
            result.printSummary();
        }
        
        System.out.println();
        
        // Find best performers
        BenchmarkResult bestThroughput = results.stream()
            .max(Comparator.comparing(BenchmarkResult::getThroughputMessagesPerSec))
            .orElse(null);
        
        BenchmarkResult bestLatency = results.stream()
            .min(Comparator.comparing(BenchmarkResult::getP95LatencyMs))
            .orElse(null);
        
        if (bestThroughput != null) {
            System.out.println("🏆 Best Throughput: " + bestThroughput.getConfigName() + 
                             String.format(" (%.2f msg/sec)", bestThroughput.getThroughputMessagesPerSec()));
        }
        
        if (bestLatency != null) {
            System.out.println("⚡ Best Latency:    " + bestLatency.getConfigName() + 
                             String.format(" (%d ms p95)", bestLatency.getP95LatencyMs()));
        }
        
        System.out.println();
    }
    
    /**
     * Main method for running benchmarks from command line.
     */
    public static void main(String[] args) {
        ThroughputBenchmark benchmark = new ThroughputBenchmark();
        
        // Configuration
        String bootstrapServers = System.getProperty("KAFKA_BOOTSTRAP_SERVERS",
                                   System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        String topicName = "performance-benchmark-topic";
        int messageCount = 10000;
        int messageSizeBytes = 1024; // 1KB messages
        
        System.out.println("╔═══════════════════════════════════════════════════════════╗");
        System.out.println("║  Kafka Producer Performance Benchmark                     ║");
        System.out.println("╚═══════════════════════════════════════════════════════════╝");
        System.out.println("\nBootstrap Servers: " + bootstrapServers);
        System.out.println("Topic:             " + topicName);
        System.out.println("Message Count:     " + String.format("%,d", messageCount));
        System.out.println("Message Size:      " + String.format("%,d", messageSizeBytes) + " bytes");
        System.out.println();
        
        // Ensure topic exists
        ensureTopicExists(bootstrapServers, topicName, 6, (short) 1);
        
        // Run compression comparison
        List<BenchmarkResult> compressionResults = benchmark.compareCompression(
            topicName, messageCount, messageSizeBytes);
        
        printComparisonTable(compressionResults);
        
        // Run batch size comparison
        List<BenchmarkResult> batchResults = benchmark.compareBatchSizes(
            topicName, messageCount, messageSizeBytes);
        
        printComparisonTable(batchResults);
        
        System.out.println("\n✓ Benchmark complete!");
    }
}
