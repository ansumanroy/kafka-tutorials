package com.kafkatutorials.partitioning;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Analyzes message distribution across topic partitions.
 * 
 * Provides insights into:
 * - Message count per partition
 * - Partition size distribution
 * - Hot partition detection
 * - Key distribution analysis
 */
public class PartitionDistributionAnalyzer {
    
    private static final Logger logger = LoggerFactory.getLogger(PartitionDistributionAnalyzer.class);
    private final String bootstrapServers;
    
    public PartitionDistributionAnalyzer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
    
    /**
     * Analyze distribution for a topic.
     */
    public DistributionReport analyzeDistribution(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        
        try (AdminClient admin = AdminClient.create(props)) {
            // Get topic description
            DescribeTopicsResult describeResult = admin.describeTopics(Collections.singleton(topicName));
            TopicDescription description = describeResult.all().get().get(topicName);
            
            int partitionCount = description.partitions().size();
            logger.info("Analyzing topic: {} with {} partitions", topicName, partitionCount);
            
            // Get partition offsets
            Map<Integer, PartitionInfo> partitionInfos = new HashMap<>();
            List<TopicPartition> topicPartitions = new ArrayList<>();
            
            for (int i = 0; i < partitionCount; i++) {
                topicPartitions.add(new TopicPartition(topicName, i));
                partitionInfos.put(i, new PartitionInfo(i));
            }
            
            // Get end offsets
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                            "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                            "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, 
                            "distribution-analyzer-" + UUID.randomUUID());
            
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
                
                long totalMessages = 0;
                
                for (TopicPartition tp : topicPartitions) {
                    long start = beginningOffsets.get(tp);
                    long end = endOffsets.get(tp);
                    long count = end - start;
                    
                    PartitionInfo info = partitionInfos.get(tp.partition());
                    info.setStartOffset(start);
                    info.setEndOffset(end);
                    info.setMessageCount(count);
                    
                    totalMessages += count;
                }
                
                return new DistributionReport(topicName, partitionInfos, totalMessages);
            }
            
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Error analyzing distribution", e);
            throw new RuntimeException("Failed to analyze distribution", e);
        }
    }
    
    /**
     * Distribution report container.
     */
    public static class DistributionReport {
        private final String topicName;
        private final Map<Integer, PartitionInfo> partitions;
        private final long totalMessages;
        
        public DistributionReport(String topicName, 
                                 Map<Integer, PartitionInfo> partitions, 
                                 long totalMessages) {
            this.topicName = topicName;
            this.partitions = partitions;
            this.totalMessages = totalMessages;
        }
        
        public String getTopicName() {
            return topicName;
        }
        
        public Map<Integer, PartitionInfo> getPartitions() {
            return partitions;
        }
        
        public long getTotalMessages() {
            return totalMessages;
        }
        
        public int getPartitionCount() {
            return partitions.size();
        }
        
        public long getAverageMessagesPerPartition() {
            if (partitions.isEmpty()) return 0;
            return totalMessages / partitions.size();
        }
        
        public PartitionInfo getMaxPartition() {
            return partitions.values().stream()
                .max(Comparator.comparing(PartitionInfo::getMessageCount))
                .orElse(null);
        }
        
        public PartitionInfo getMinPartition() {
            return partitions.values().stream()
                .min(Comparator.comparing(PartitionInfo::getMessageCount))
                .orElse(null);
        }
        
        public double getStandardDeviation() {
            long avg = getAverageMessagesPerPartition();
            double variance = partitions.values().stream()
                .mapToDouble(p -> Math.pow(p.getMessageCount() - avg, 2))
                .average()
                .orElse(0.0);
            return Math.sqrt(variance);
        }
        
        public boolean hasHotPartitions(double threshold) {
            long avg = getAverageMessagesPerPartition();
            return partitions.values().stream()
                .anyMatch(p -> p.getMessageCount() > avg * threshold);
        }
        
        /**
         * Print detailed distribution report.
         */
        public void print() {
            System.out.println("\n╔═══════════════════════════════════════════════════════════╗");
            System.out.println("║  Partition Distribution Analysis                          ║");
            System.out.println("╚═══════════════════════════════════════════════════════════╝");
            
            System.out.println("\n📊 Topic: " + topicName);
            System.out.println("Total Messages: " + String.format("%,d", totalMessages));
            System.out.println("Partition Count: " + getPartitionCount());
            System.out.println("Average per Partition: " + String.format("%,d", getAverageMessagesPerPartition()));
            System.out.println("Standard Deviation: " + String.format("%.2f", getStandardDeviation()));
            
            System.out.println("\n📈 Distribution by Partition:");
            System.out.println(String.format("%-10s | %-12s | %-10s | %-30s", 
                              "Partition", "Messages", "Percentage", "Distribution"));
            System.out.println("─".repeat(75));
            
            // Sort by partition number
            partitions.values().stream()
                .sorted(Comparator.comparing(PartitionInfo::getPartition))
                .forEach(p -> {
                    double pct = p.getPercentage(totalMessages);
                    int barLength = (int) Math.round(pct / 2.0); // 50 chars = 100%
                    String bar = "█".repeat(Math.max(0, barLength));
                    
                    System.out.printf("%-10d | %,12d | %9.2f%% | %s\n",
                                     p.getPartition(),
                                     p.getMessageCount(),
                                     pct,
                                     bar);
                });
            
            System.out.println();
            
            // Hot partition detection
            PartitionInfo max = getMaxPartition();
            PartitionInfo min = getMinPartition();
            
            if (max != null && min != null) {
                System.out.println("🔥 Hot/Cold Analysis:");
                System.out.printf("  • Hottest partition: %d (%,d messages, %.1f%%)\n",
                                 max.getPartition(),
                                 max.getMessageCount(),
                                 max.getPercentage(totalMessages));
                System.out.printf("  • Coldest partition: %d (%,d messages, %.1f%%)\n",
                                 min.getPartition(),
                                 min.getMessageCount(),
                                 min.getPercentage(totalMessages));
                
                double ratio = (double) max.getMessageCount() / 
                              (min.getMessageCount() > 0 ? min.getMessageCount() : 1);
                System.out.printf("  • Hottest/Coldest ratio: %.2fx\n", ratio);
                
                if (hasHotPartitions(2.0)) {
                    System.out.println("\n⚠️  WARNING: Hot partition detected!");
                    System.out.println("   Some partitions have >2x average load.");
                    System.out.println("   Consider using composite keys or custom partitioner.");
                }
            }
            
            System.out.println();
        }
        
        /**
         * Print compact summary.
         */
        public void printSummary() {
            PartitionInfo max = getMaxPartition();
            PartitionInfo min = getMinPartition();
            
            System.out.printf("%-25s | %,10d msgs | %3d partitions | Max: %d (%.1f%%) | Min: %d (%.1f%%) | StdDev: %.2f\n",
                             topicName,
                             totalMessages,
                             getPartitionCount(),
                             max != null ? max.getPartition() : -1,
                             max != null ? max.getPercentage(totalMessages) : 0.0,
                             min != null ? min.getPartition() : -1,
                             min != null ? min.getPercentage(totalMessages) : 0.0,
                             getStandardDeviation());
        }
    }
}
